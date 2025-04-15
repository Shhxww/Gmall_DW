import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-15 20:16:40
 * 1）启动zk、kafka、maxwell
 * 2）运行DwdTradeRefundPaySucDetail
 **/

public class DwdTradeRefundPaySucDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018,4, Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  设置订单状态保留时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30));
//        TODO  读取kafka上的业务数据，映射成topic_db动态表
        FlinkSQlUtil.readOdsData(tEnv,Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);

//        TODO  过滤出退款表的数据，并映射成表
        Table refundPayment = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['payment_type'] payment_type," +
                "data['callback_time'] callback_time," +
                "data['total_amount'] total_amount," +
                "pt, " +
                "ts " +
                "from topic_db " +
                "where `table`='refund_payment' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='1602'");
        tEnv.createTemporaryView("refund_payment", refundPayment);

//        TODO  过滤出退款成功的订单表数据
        Table orderInfo = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['province_id'] province_id " +
                "from topic_db " +
                "where `table`='order_info' " +
                "and `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and `data`['order_status']='1006'");

        tEnv.createTemporaryView("order_info", orderInfo);

//        TODO  过滤出退款成功的退单表数据
                Table orderRefundInfo = tEnv.sqlQuery(
            "select " +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_num'] refund_num " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_refund_info' " +
                "and `type`='update' " +
                "and `old`['refund_status'] is not null " +
                "and `data`['refund_status']='0705'");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        TODO  读取Hbase上的字典表，并映射成表
        FlinkSQlUtil.readHbaseDic(tEnv);

//        TODO  将4表进行join
        Table result = tEnv.sqlQuery("select " +
                "rp.id," +
                "oi.user_id," +
                "rp.order_id," +
                "rp.sku_id," +
                "oi.province_id," +
                "rp.payment_type," +
                "dic.info.dic_name payment_type_name," +
                "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                "rp.callback_time," +
                "ori.refund_num," +
                "rp.total_amount," +
                "rp.ts " +
                "from refund_payment rp " +
                "join order_refund_info ori " +
                "on rp.order_id=ori.order_id and rp.sku_id=ori.sku_id " +
                "join order_info oi on rp.order_id=oi.id " +
                "join base_dic for system_time as of rp.pt as dic on rp.payment_type=dic.dic_code "
        );

//        TODO  创建kafka退款事实表映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint," +
                "primary key(id) not enforced " +
                ")" + FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

//        TODO  将数据写入映射表
        result.executeInsert(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS);


    }
}
