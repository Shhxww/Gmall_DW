import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @基本功能:   交易域--支付事实表（一次成功的支付操作里的一种商品）
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-16 08:24:00
 * 启动zk、kafka、maxwell
 * 运行DwdTradeOrderDetail及 DwdTradeOrderPaySucDetail
 **/

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
//        启动程序
        new DwdTradeOrderPaySucDetail().start(
                10016,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  1、设置ttl，防止延迟
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

//        TODO  2、读取kafka上的业务数据，映射成topic_db表
        FlinkSQlUtil.readOdsData(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

//        TODO  3、过滤出支付表的数据，映射成表
        Table paymentInfo = tEnv.sqlQuery("select " +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "`pt`," +
                "ts, " +
                "et " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='payment_info' " +
                "and `type`='update' " +
                "and `old`['payment_status'] is not null " +
                "and `data`['payment_status']='1602' ");
        tEnv.createTemporaryView("payment_info", paymentInfo);

//        TODO  4、读取下单事实表，映射成表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"(" +
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
                "ts bigint," +
//                由于这里的ts是bigint类型，要转化成timestamp类型，maxwell 提取的ts是秒级，所以参数为0(毫秒级就为 3 )
                "et as to_timestamp_ltz(ts, 0), " +
                "watermark for et as et - interval '3' second " + //设置水位线，乱序程度为3s
                ")"+FlinkSQlUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

//        TODO  读取Hbase上的字典表
        FlinkSQlUtil.readHbaseDic(tEnv);

//        TODO  将支付表与下单事实表进行inevter join，再与字典表进行 lookup join
        Table result = tEnv.sqlQuery("select " +
                "od.id order_detail_id," +
                "od.order_id," +
                "od.user_id," +
                "od.sku_id," +
                "od.sku_name," +
                "od.province_id," +
                "od.activity_id," +
                "od.activity_rule_id," +
                "od.coupon_id," +
                "pi.payment_type payment_type_code ," +
                "dic.dic_name payment_type_name," +
                "pi.callback_time," +
                "od.sku_num," +
                "od.split_original_amount," +
                "od.split_activity_amount," +
                "od.split_coupon_amount," +
                "od.split_total_amount split_payment_amount," +
                "pi.ts " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id=od.order_id " +
                "and od.et >= pi.et - interval '30' minute " +
                "and od.et <= pi.et + interval '5' second " +
                "join base_dic for system_time as of pi.pt as dic " +
                "on pi.payment_type=dic.dic_code ");

//        TODO  创建kafka支付事实表映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS+"(" +
                            "order_detail_id string," +
                            "order_id string," +
                            "user_id string," +
                            "sku_id string," +
                            "sku_name string," +
                            "province_id string," +
                            "activity_id string," +
                            "activity_rule_id string," +
                            "coupon_id string," +
                            "payment_type_code string," +
                            "payment_type_name string," +
                            "callback_time string," +
                            "sku_num string," +
                            "split_original_amount string," +
                            "split_activity_amount string," +
                            "split_coupon_amount string," +
                            "split_payment_amount string," +
                            "ts bigint, " +
                            "primary key(order_detail_id) not enforced " +
                            ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

//        TODO  将数据插入到支付事实表映射表去
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

    }
}
