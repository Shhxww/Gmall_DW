package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.UserLoginBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @基本功能:   用户域---用户注册、用户回流窗口汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 15:24:48
 **/

public class DwsUserUserLoginWindow extends BaseApp {

    public static void main(String[] args) {
//        启动程序
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                Constant.TOPIC_DWD_TRAFFIC_PAGE,
                "dws_user_user_login_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO  1、读取用户登录明细事实表数据包，并进行清洗转化
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonstr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonstr);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                if (StringUtils.isNotEmpty(uid)&&(StringUtils.isEmpty(last_page_id)||"login".equals(last_page_id))){
                    out.collect(jsonObject);
                }
            }
        });
//        TODO  2、按照uid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObj.keyBy(obj -> obj.getJSONObject("common").getString("uid"));
//        TODO  3、进行回流用户，新用户标记标记
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

            private ValueState<String> lastLoginDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDate", String.class));
            }

            @Override
            public void processElement(JSONObject obj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                Long ts = obj.getLong("ts");
                String today = DateFormatUtil.tsToDate(ts);
                String lastLoginDate = lastLoginDateState.value();
                Long uuCt = 0L;
                Long backCt = 0L;
                if (!today.equals(lastLoginDate)) {
                    // 今天的第一次登录
                    uuCt = 1L;
                    lastLoginDateState.update(today);

                    // 计算回流: 曾经登录过
                    if (lastLoginDate != null) { //
                        long lastLoginTs = DateFormatUtil.dateToTs(lastLoginDate);
                        // 7日回流
                        if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 7) {
                            backCt = 1L;
                        }
                    }
                }

                if (uuCt == 1) {
                    out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });
//        TODO  4、设置水位线
        SingleOutputStreamOperator<UserLoginBean> beanWM = beanDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((bs, ts) -> bs.getTs()));
//        TODO  5、开窗
        AllWindowedStream<UserLoginBean, TimeWindow> beanWindows = beanWM.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));
//        TODO  6、聚合
        SingleOutputStreamOperator<UserLoginBean> result = beanWindows.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                UserLoginBean bean = values.iterator().next();
                bean.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                bean.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                bean.setCurDate(DateFormatUtil.tsToDateForPartition(context.window().getStart()));
                out.collect(bean);
            }
        });
//        TODO  7、输出到Doris
//        result.print();
        result.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_user_user_login_window"));
    }
}
