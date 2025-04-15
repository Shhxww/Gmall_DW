package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TradeProvinceOrderBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.AsyncDimFunction;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 22:08:29
 **/

public class DwsTradeProvinceOrderWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,
                "dws_trade_province_order_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO  读取订单明细实时表数据，转换为统计类型
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = kafkaDS.process(new ProcessFunction<String, TradeProvinceOrderBean>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                HashSet<String> set = new HashSet<>();
                set.add(jsonObject.getString("order_id"));
                out.collect(
                        TradeProvinceOrderBean
                                .builder()
                                .provinceId(jsonObject.getString("province_id"))
                                .orderIdSet(set)
                                .orderAmount(jsonObject.getBigDecimal("split_total_amount"))
                                .orderDetailId(jsonObject.getString("id"))
                                .ts(jsonObject.getLong("ts") * 1000)  //maxwell 导入的是秒级的时间戳，要*1000变毫秒级
                                .build()
                );
            }
        });
//        TODO  按照订单明细id进行分组
        KeyedStream<TradeProvinceOrderBean, String> beankeyed = beanDS.keyBy(bs -> bs.getOrderDetailId()) ;
//        TODO  进行去重
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDis = beankeyed.process(new ProcessFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>() {

            private ValueState<TradeProvinceOrderBean> lastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<TradeProvinceOrderBean> isFirst = new ValueStateDescriptor<>("isFirst", TradeProvinceOrderBean.class);
                isFirst.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build());
                lastState = getRuntimeContext().getState(isFirst);
            }

            @Override
            public void processElement(TradeProvinceOrderBean bean, ProcessFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>.Context ctx, Collector<TradeProvinceOrderBean> out) throws Exception {

                TradeProvinceOrderBean laststate = lastState.value();

                if (laststate == null) {
                    lastState.update(bean);
                    out.collect(bean);
                } else {
                    bean.setOrderAmount(laststate.getOrderAmount().subtract(bean.getOrderAmount()));
                    lastState.update(bean);
                    out.collect(bean);

                }
            }
        });
//        TODO
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanWM = beanDis.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((bs, ts) -> bs.getTs()));
//        TODO
        KeyedStream<TradeProvinceOrderBean, String> beanK = beanWM.keyBy(TradeProvinceOrderBean::getProvinceId);
//        TODO
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> beanWindows = beanK.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//        TODO
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduce = beanWindows.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> elements, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean bs = elements.iterator().next();
                        bs.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                        bs.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                        bs.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                        bs.setOrderCount((long) bs.getOrderIdSet().size());
                        out.collect(bs);
                    }
                }
        );
//        TODO
        SingleOutputStreamOperator<TradeProvinceOrderBean> result = AsyncDataStream.unorderedWait(
                reduce,
                new AsyncDimFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getRowKey(TradeProvinceOrderBean bean) {
                        return bean.getProvinceId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public void addDims(TradeProvinceOrderBean bean, JSONObject dim) {
                        if (dim!=null) {
                            bean.setProvinceName(dim.getString("name"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        TODO
        result.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_trade_province_order_window"));
    }
}
