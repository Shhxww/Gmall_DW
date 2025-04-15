import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-15 15:42:14
 **/

public class DwdTradeOrderCancelDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(
                10015,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  设置ttl，防止延迟
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

//        TODO  读取kafka上的业务数据，映射为topic_db表
        FlinkSQlUtil.readOdsData(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

//        TODO  过滤出取消订单表的数据，并映射成order_cancel表
        Table orderCancel = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['operate_time'] operate_time, " +
                " `ts` " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status']='1001' " +
                "and `data`['order_status']='1003' ");
        tEnv.createTemporaryView("order_cancel",orderCancel);

//        TODO  读取下单事实表
        tEnv.executeSql(
            "create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint " +
                ")" + FlinkSQlUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

//        TODO  两表进行join，得到取消订单事实表数据
        Table result = tEnv.sqlQuery("select  " +
                "od.id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "date_format(oc.operate_time, 'yyyy-MM-dd') order_cancel_date_id," +
                "oc.operate_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount," +
                "oc.ts " +
                "from dwd_trade_order_detail od " +
                "join order_cancel oc " +
                "on od.order_id=oc.id ");

//        TODO  创建kafka取消订单事实表映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_CANCEL+"(" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "cancel_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint, " +
                "primary key(id) not enforced " +
                ")" +FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));

//        TODO  将数据插入到映射表去
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);

    }
}
