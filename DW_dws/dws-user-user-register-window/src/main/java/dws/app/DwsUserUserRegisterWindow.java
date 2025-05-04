package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.UserRegisterBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @基本功能:   用户注册窗口汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 20:16:59
 **/

public class DwsUserUserRegisterWindow extends BaseApp {

    public static void main(String[] args) {
//         启动程序
         new DwsUserUserRegisterWindow().start(
                10025,
                4,
                 Constant.TOPIC_DWD_USER_REGISTER,
                 "dws_user_user_register_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO  1、读取用户注册事实表数据，并转换为jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSONObject::parseObject);
//        TODO  2、设置水位线
        SingleOutputStreamOperator<JSONObject> dS = jsonObj.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((jsO, ts) -> jsO.getLong("create_time"))
        );
//        TODO  3、开窗
        AllWindowedStream<JSONObject, TimeWindow> jsonWindows = dS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        TODO  4、聚合
        SingleOutputStreamOperator<UserRegisterBean> aggregate =
            jsonWindows.aggregate(new AggregateFunction<JSONObject, Long, Long>() {
                                @Override
                                public Long createAccumulator() {
                                    return 0L;
                                }

                                @Override
                                public Long add(JSONObject value, Long acc) {
                                    return acc + 1;
                                }

                                @Override
                                public Long getResult(Long acc) {
                                    return acc;
                                }

                                @Override
                                public Long merge(Long acc1, Long acc2) {
                                    return acc1 + acc2;
                                }},new ProcessAllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                                @Override
                                public void process(Context ctx,
                                                    Iterable<Long> elements,
                                                    Collector<UserRegisterBean> out) throws Exception {
                                    Long result = elements.iterator().next();

                                    out.collect(
                                            new UserRegisterBean(
                                                    DateFormatUtil.tsToDateTime(ctx.window().getStart()),
                                                DateFormatUtil.tsToDateTime(ctx.window().getEnd()),
                                                DateFormatUtil.tsToDateForPartition(ctx.window().getEnd()),
                                                result
                                            )
                                    );

                                }
                            }
                    );
//        TODO  5、输出到Doris
        aggregate
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_user_user_register_window"));
    }
}
