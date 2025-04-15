package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TradeOrderBean;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 21:45:41
 **/

public class DwsTradeOrderWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
            10028,
            4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,
                "dws_trade_order_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSONObject::parseObject);
//        TODO
        KeyedStream<JSONObject, String> keyedDS = jsonObj.keyBy(jsonObject -> jsonObject.getString("user_id"));
//        TODO
        SingleOutputStreamOperator<TradeOrderBean> process = keyedDS.process(new ProcessFunction<JSONObject, TradeOrderBean>() {

            private ValueState<String> lastOrderDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
            }

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                long ts = value.getLong("ts") * 1000;

                String today = DateFormatUtil.tsToDate(ts);
                String lastOrderDate = lastOrderDateState.value();

                long orderUu = 0L;
                long orderNew = 0L;

                if (!today.equals(lastOrderDate)) {
                    orderUu = 1L;
                    lastOrderDateState.update(today);
//
                    if (lastOrderDate == null) {
                        orderNew = 1L;
                    }

                }
//
                if (orderUu == 1) {
                    out.collect(
                            new TradeOrderBean
                                    ("", "", "", orderUu, orderNew, ts)
                    );
                }
            }
        });
//        TODO
        SingleOutputStreamOperator<TradeOrderBean> beanWM = process
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bs, ts) -> bs.getTs()));
//        TODO
        AllWindowedStream<TradeOrderBean, TimeWindow> beanWindows = beanWM.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        TODO
        SingleOutputStreamOperator<TradeOrderBean> result = beanWindows.reduce(
                new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1,
                                                 TradeOrderBean value2) {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<TradeOrderBean> elements,
                                        Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                        out.collect(bean);
                    }
                }
        );
//        TODO
        result.print();
        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_trade_order_window"));
//        TODO
    }
}
