import Gmall_fs.base.BaseApp;
import Gmall_fs.bean.TradeSkuOrderBean;
import Gmall_fs.constant.Constant;
import Gmall_fs.function.AsyncDimFunction;
import Gmall_fs.function.DorisMapFunction;
import Gmall_fs.util.DateFormatUtil;
import Gmall_fs.util.FlinkSinkUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * @基本功能:    下单轻度汇总表（维度：商品id，数值：原始金额、活动减免、优惠劵减免、实付金额）
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-20 21:58:32
 **/

public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {

    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache()
                .start(
                        10029,
                        4,
                        Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,
                        "order_dws"
                );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//         数据格式：
//         {
//              "create_time":"2025-04-21 14:26:30",
//              "sku_num":"1",
//              "split_original_amount":"488.0000",
//              "split_coupon_amount":"0.0",
//              "sku_id":"33",
//              "date_id":"2025-04-21",
//              "user_id":"1349",
//              "province_id":"30",
//              "sku_name":"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml",
//              "id":"13967",
//              "order_id":"9827",
//              "split_activity_amount":"0.0",
//              "split_total_amount":"488.0",
//              "ts":1745216790
//              }

 //    TODO  读取下单事实表数据，过滤掉空值，封装成jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (StringUtils.isNotEmpty(jsonStr)) {
                    JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                    out.collect(jsonObject);
                }
            }
        });
//    TODO  按照订单明细 id分组
        KeyedStream<JSONObject, String> jsonObjkeyed = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
//    TODO  对数据进行去重，并将数据转换成统计类型
        SingleOutputStreamOperator<TradeSkuOrderBean> tradeSkuDS = jsonObjkeyed.map(new RichMapFunction<JSONObject, TradeSkuOrderBean>() {

            ValueState<TradeSkuOrderBean> tradeSkuOrderBeanValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<TradeSkuOrderBean> stateProperties = new ValueStateDescriptor<TradeSkuOrderBean>("tradeSkuOrderBean", TradeSkuOrderBean.class);
                stateProperties.enableTimeToLive(new StateTtlConfig.Builder(org.apache.flink.api.common.time.Time.seconds(10)).build());
                tradeSkuOrderBeanValueState = getRuntimeContext().getState(stateProperties);
            }

            @Override
            public TradeSkuOrderBean map(JSONObject obj) throws Exception {
                TradeSkuOrderBean laststate = tradeSkuOrderBeanValueState.value();
                if (laststate != null) {
                    TradeSkuOrderBean skuOrderBean =
                            TradeSkuOrderBean
                                    .builder()
                                    .skuId(obj.getString("sku_id"))
                                    .orderDetailId(obj.getString("id"))
                                    .originalAmount(obj.getBigDecimal("split_original_amount").subtract(laststate.getOriginalAmount()))
                                    .orderAmount(obj.getBigDecimal("split_total_amount").subtract(laststate.getOrderAmount()))
                                    .activityReduceAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_activity_amount").subtract(laststate.getActivityReduceAmount()))
                                    .couponReduceAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_coupon_amount").subtract(laststate.getCouponReduceAmount()))
                                    .ts(obj.getLong("ts") * 1000)
                                    .build();
                     tradeSkuOrderBeanValueState.update(skuOrderBean);
                    return skuOrderBean;
                } else {
                    TradeSkuOrderBean skuOrderBean =
                            TradeSkuOrderBean
                                    .builder()
                                    .skuId(obj.getString("sku_id"))
                                    .orderDetailId(obj.getString("id"))
                                    .originalAmount(obj.getBigDecimal("split_original_amount"))
                                    .orderAmount(obj.getBigDecimal("split_total_amount"))
                                    .activityReduceAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_activity_amount"))
                                    .couponReduceAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_coupon_amount"))
                                    .ts(obj.getLong("ts") * 1000)
                                    .build();
                    tradeSkuOrderBeanValueState.update(skuOrderBean);
                    return skuOrderBean;
                }
            }
        });
//    TODO  设计水位线
        SingleOutputStreamOperator<TradeSkuOrderBean> skubeanWM = tradeSkuDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<TradeSkuOrderBean>) (element, recordTimestamp) -> element.getTs()));
//    TODO  按照  分组
        KeyedStream<TradeSkuOrderBean, String> skuWMkeyed = skubeanWM.keyBy(tr -> tr.getSkuId());
//    TODO  开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> window = skuWMkeyed.window(TumblingEventTimeWindows.of(seconds(10)));
//    TODO  聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduce = window.reduce(new ReduceFunction<TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                TradeSkuOrderBean t = elements.iterator().next();
                t.setStt(DateFormatUtil.tsToDateTime(context.window().getStart()));
                t.setEdt(DateFormatUtil.tsToDateTime(context.window().getEnd()));
                t.setCurDate(DateFormatUtil.tsToDate(context.window().getStart()));
                out.collect(t);
            }
        });

//    TODO  进行维度关联  (旁路缓存、异步 IO )

//        与dim_sku_info维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDIM_1 = AsyncDataStream.unorderedWait(reduce,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        if (dim != null){
                            bean.setSkuName(dim.getString("sku_name"));
                            bean.setSpuId(dim.getString("spu_id"));
                            bean.setTrademarkId(dim.getString("tm_id"));
                            bean.setCategory3Id(dim.getString("category3_id"));
                        }
                    }
                }, 120, TimeUnit.SECONDS);
//        与din_spu_info维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDIM_2 = AsyncDataStream.unorderedWait(skuDIM_1,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getSpuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setSpuName(dim.getString("spu_name"));
                    }
                }, 120, TimeUnit.SECONDS);
//        与dim_base_trademark维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDIM_3 = AsyncDataStream.unorderedWait(skuDIM_2,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                }, 120, TimeUnit.SECONDS);
//        与dim_base_category3维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDIM_4 = AsyncDataStream.unorderedWait(skuDIM_3,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getCategory3Id();
                }

                @Override
                public String getTableName() {
                    return "dim_base_category3";
                }

                @Override
                public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setCategory3Name(dim.getString("name"));
                    bean.setCategory2Id(dim.getString("category2_id"));
                }
            }, 120, TimeUnit.SECONDS);
//        与dim_base_category2维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> skuDIM_5 = AsyncDataStream.unorderedWait(skuDIM_4,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getCategory2Id();
                }

                @Override
                public String getTableName() {
                    return "dim_base_category2";
                }

                @Override
                public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                    bean.setCategory2Name(dim.getString("name"));
                    bean.setCategory1Id(dim.getString("category1_id"));
                }
            }, 120, TimeUnit.SECONDS);
//        与dim_base_category1维度表进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> sku_DIM = AsyncDataStream.unorderedWait(skuDIM_5,
                new AsyncDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                }, 120, TimeUnit.SECONDS);

//    TODO  写入doris
        sku_DIM
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("gmall.dws_trade_sku_order_window"));
//    TODO
//    TODO
//    TODO
//    TODO
//    TODO
    }
}
