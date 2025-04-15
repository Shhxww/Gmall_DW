package dws.app;

import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TradeTrademarkCategoryUserRefundBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.AsyncDimFunction;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-23 23:04:02
 **/

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10031,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND,
                "dws_trade_trademark_category_user_refund_window"
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDS = kafkaDS.process(new ProcessFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public void processElement(String jsonstr, ProcessFunction<String, TradeTrademarkCategoryUserRefundBean>.Context ctx, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                JSONObject obj = JSONObject.parseObject(jsonstr);
                out.collect(
                        TradeTrademarkCategoryUserRefundBean.builder()
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .skuId(obj.getString("sku_id"))
                                .userId(obj.getString("user_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .build()
                );
            }
        });
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWkey = AsyncDataStream.unorderedWait(
                beanDS,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setTrademarkId(dim.getString("tm_id"));
                        bean.setCategory3Id(dim.getString("category3_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWM = beanWkey.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((bs, ts) -> bs.getTs()));
//        TODO
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> beanKeyed = beanWM.keyBy(bean -> bean.getUserId() + "_" + bean.getCategory3Id() + "_" + bean.getTrademarkId());
//        TODO
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> beanWindows = beanKeyed.window(TumblingEventTimeWindows.of(Time.seconds(10)));
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduce = beanWindows.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1,
                                                                       TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String s,
                                        Context ctx,
                                        Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                        Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();

                        bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                        bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));  // doris 的分区字段: 年月日带连字符也可以

                        bean.setRefundCount((long) bean.getOrderIdSet().size());

                        out.collect(bean);
                    }
                }
        );
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDIm_1 = AsyncDataStream.unorderedWait(
                reduce,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        TODO
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDim_2 = AsyncDataStream.unorderedWait(
                beanDIm_1,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDim_3 = AsyncDataStream.unorderedWait(
                beanDim_2,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = AsyncDataStream.unorderedWait(
                beanDim_3,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
//        TODO
        resultStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_trade_trademark_category_user_refund_window"));
    }
}
