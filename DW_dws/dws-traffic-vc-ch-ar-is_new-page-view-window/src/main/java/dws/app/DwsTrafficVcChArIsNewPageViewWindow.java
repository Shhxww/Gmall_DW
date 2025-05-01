package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TrafficPageViewBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import Gmall_fs.util.FlinkSourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.time.Duration;

/**
 * @基本功能:   流量域--当天独立用户窗口汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-19 16:54:13
 **/

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {

    public static void main(String[] args) {
//        启动程序
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10021,
                4,
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "dws_traffic_vc_ch_ar_is_new_page_view_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {

//        {
    //        "common":{"ar":"28","uid":"1227","os":"Android 13.0","ch":"web","is_new":"1","md":"vivo IQOO Z6x ","mid":"mid_62","vc":"v2.1.132","ba":"vivo","sid":"190c1a34-1343-41a4-ba96-670772dfb402"},
    //        "page":{"page_id":"order","item":"20","during_time":14101,"item_type":"sku_ids","last_page_id":"good_detail"},
    //        "ts":1717856903000
//        }
 //        TODO  1、读取页面日志数据转化成jsonObj流
        SingleOutputStreamOperator<JSONObject> pageMap = kafkaDS.map(JSONObject::parseObject);
//        TODO  2、按设备id进行分组
        KeyedStream<JSONObject, String> pageKB = pageMap.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
//        pageKB.print("pagekkk: ");

//        TODO  3、统计各个指标，转换为TrafficPageViewBean数据类型
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = pageKB.map(new RichMapFunction<JSONObject, TrafficPageViewBean>() {
//            定义一个状态，接收设备最后一次访问的时间
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
//                对状态进行初始化，设置状态时间，一天之后过期
                ValueStateDescriptor<String> lastVisitDateDescriptor = new ValueStateDescriptor<>("lastVisitDate", String.class);
                lastVisitDateDescriptor.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1)).build());
                lastVisitDateState = getRuntimeContext().getState(lastVisitDateDescriptor);
            }

            @Override
            public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
//                提取出时间字段
                Long ts = jsonObj.getLong("ts");
                String curdate = DateFormatUtil.tsToDate(ts);
//                提取出页面内容
                JSONObject page = jsonObj.getJSONObject("page");
//                提取出数据内容
                JSONObject common = jsonObj.getJSONObject("common");
//                提取出设备最后一次访问时间
                String lastVisitDate = lastVisitDateState.value();
//                定义：uv 是否为独立用户
                Long uv = 0L;
//                页面访问次数
                String lastPageId = page.getString("last_page_id");
                Long sv = org.apache.commons.lang3.StringUtils.isEmpty(lastPageId) ? 1L : 0L;
//                获取持续访问时间
                Long durT = page.getLong("during_time");
//                判断设备之前是否访问过 || 当前访问时间与最后访问时间不同
                if (StringUtils.isNullOrWhitespaceOnly(lastVisitDate) || !curdate.equals(lastVisitDate)) {
                    uv = 1L;
                    lastVisitDateState.update(curdate);
                }
//                若为当天独立用户访问，将该数据进行转化并往下游传递
                    return new TrafficPageViewBean(
                            "", "", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            uv,
                            sv,
                            1L,
                            durT,
                            ts,
                            common.getString("sid")
                    );

            }
        });

//        TODO  4、设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> beanW = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );
//        TODO  5、按照维度分组
        KeyedStream<TrafficPageViewBean, String> beankeyed = beanW.keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew());
//        beankeyed.print();
//        TODO  6、开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> window = beankeyed.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)));
//        TODO  7、进行聚合
        SingleOutputStreamOperator<TrafficPageViewBean> result = window.reduce(new ReduceFunction<TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                    value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                    value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                    value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                    value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                    return value1;
                }
            }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                    TrafficPageViewBean trafficPageViewBean = input.iterator().next();
                    trafficPageViewBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                    trafficPageViewBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                    trafficPageViewBean.setCur_date(DateFormatUtil.tsToDate(window.getStart()));
                    out.collect(trafficPageViewBean);
                }
            });

//        TODO  8、写入doris
        result.map(new DorisMapFunction<TrafficPageViewBean>()).sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
