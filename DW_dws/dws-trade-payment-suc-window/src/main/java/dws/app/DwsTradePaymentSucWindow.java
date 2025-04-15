package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TradePaymentBean;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @基本功能:交易域---支付成功各窗口汇总表
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 21:07:38
 **/

public class DwsTradePaymentSucWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                10027,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS,
                "dws_trade_payment_suc_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO
        SingleOutputStreamOperator<JSONObject> jsonObj = kafkaDS.map(JSONObject::parseObject);
//        TODO
        jsonObj.print();
        KeyedStream<JSONObject, String> keyedDS = jsonObj.keyBy(jsonobj -> jsonobj.getString("user_id"));
//        TODO
        SingleOutputStreamOperator<TradePaymentBean> beanDS = keyedDS.process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

            private ValueState<String> lastPayDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastPayDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPayDate", String.class));
            }

            @Override
            public void processElement(JSONObject obj,
                                       Context ctx,
                                       Collector<TradePaymentBean> out) throws Exception {
                String lastPayDate = lastPayDateState.value();
                long ts = obj.getLong("ts") * 1000;
                String today = DateFormatUtil.tsToDate(ts);

                long payUuCount = 0L;
                long payNewCount = 0L;
                if (!today.equals(lastPayDate)) {  // 今天第一次支付成功
                    lastPayDateState.update(today);
                    payUuCount = 1L;

                    if (lastPayDate == null) {
                        // 表示这个用户曾经没有支付过, 是一个新用户支付
                        payNewCount = 1L;
                    }
                }

                if (payUuCount == 1) {
                    out.collect(new TradePaymentBean("", "", "", payUuCount, payNewCount, ts));
                }

            }
        });
//        TODO
        SingleOutputStreamOperator<TradePaymentBean> beanWm = beanDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bs, ts) -> bs.getTs())
                );
//        TODO
        AllWindowedStream<TradePaymentBean, TimeWindow> beanWindows = beanWm.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
//        TODO
        SingleOutputStreamOperator<TradePaymentBean> result = beanWindows.reduce(
                new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void process(Context ctx, Iterable<TradePaymentBean> elements, Collector<TradePaymentBean> out) throws Exception {
                        TradePaymentBean bean = elements.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));
                        out.collect(bean);
                    }
                }
        );
//        TODO
        result.print();
        result.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_trade_payment_suc_window"));
//        TODO
//        TODO
//        TODO
//        TODO
    }
}
