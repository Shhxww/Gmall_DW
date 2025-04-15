package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TrafficHomeDetailPageViewBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @基本功能:   流量域---首页、详情页页面浏览各窗口访客汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 14:30:17
 **/

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {

    public static void main(String[] args) {
//        执行程序
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "dws_traffic_home_detail_page_view_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO  读取日志数据转为 JsonObj
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSONObject::parseObject);
//        TODO  按照设备id进行分组（访客类）
        KeyedStream<JSONObject, String> keyedDS = jsonObj.keyBy(obj -> obj.getJSONObject("common").getString("mid"));
//        TODO  对数据进行判断，转换为统计类型
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeState;
            private ValueState<String> goodDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("home", String.class));
                goodDetailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetail", String.class));
            }

            @Override
            public void processElement(JSONObject obj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = obj.getJSONObject("page").getString("page_id");
                Long ts = obj.getLong("ts");
                String today = DateFormatUtil.tsToDate(ts);
                String lastHomeDate = homeState.value();
                String lastGoodDetailDate = goodDetailState.value();
                Long homeCt = 0L;
                Long goodDetailCt = 0L;
                if ("home".equals(pageId) && !today.equals(lastHomeDate)) {
                    homeCt = 1L;
                    homeState.update(today);
                } else if ("good_detail".equals(pageId) && !today.equals(lastGoodDetailDate)) {
                    goodDetailCt = 1L;
                    goodDetailState.update(today);
                }

                if (homeCt + goodDetailCt == 1) {
                    out.collect(
                            TrafficHomeDetailPageViewBean
                                    .builder()
                                    .homeUvCt(homeCt)
                                    .goodDetailUvCt(goodDetailCt)
                                    .ts(ts)
                                    .build()
                    );
                }
            }
        });
//        TODO  设置水位线
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanWM = beanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((bs, ts) -> bs.getTs()));
//        TODO  开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> beanWindows = beanWM.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));
//        TODO  聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> result = beanWindows.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                bean.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                out.collect(bean);
            }
        });
//        TODO  输出到Doris
        result.print();
        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_traffic_home_detail_page_view_window"));
    }
}
