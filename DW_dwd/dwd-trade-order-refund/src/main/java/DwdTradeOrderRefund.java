import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @基本功能:   交易域--订单退单事实表（一次退单操作里的一种商品）
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-16 10:02:35
 * 1）启动zk、kafka、maxwell
 * 2）运行DwdTradeOrderRefund
 **/

public class DwdTradeOrderRefund extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(
                10017,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  1、设置ttl，防止延迟
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

//        TODO  2、读取kafka上的业务数据，映射为topic_db表
        FlinkSQlUtil.readOdsData(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

//        TODO  3、过滤出退单表的数据，并映射成表
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                "data['id'] id," +
                "data['user_id'] user_id," +
                "data['order_id'] order_id," +
                "data['sku_id'] sku_id," +
                "data['refund_type'] refund_type," +
                "data['refund_num'] refund_num," +
                "data['refund_amount'] refund_amount," +
                "data['refund_reason_type'] refund_reason_type," +
                "data['refund_reason_txt'] refund_reason_txt," +
                "data['create_time'] create_time," +
                "pt," +
                "ts " +
                "from topic_db " +
                "where `database`='gmall' " +
                "and `table`='order_refund_info' " +
                "and `type`='insert' ");

        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

//        TODO  4、从业务数据种过滤出 ods订单表数据，并映射成表
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");
        tEnv.createTemporaryView("order_info", orderInfo);

//        TODO  5、读取Hbase上的字典表，并映射成表
        FlinkSQlUtil.readHbaseDic(tEnv);

//        TODO  6、将三表进行lookup join，得到退单事实表数据
        Table result = tEnv.sqlQuery("select " +
                "ri.id," +
                "ri.user_id," +
                "ri.order_id," +
                "ri.sku_id," +
                "oi.province_id," +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                "ri.create_time," +
                "ri.refund_type," +
                "dic1.info.dic_name," +
                "ri.refund_reason_type," +
                "dic2.info.dic_name," +
                "ri.refund_reason_txt," +
                "ri.refund_num," +
                "ri.refund_amount," +
                "ri.ts " +
                "from order_refund_info ri " +
                "join order_info oi on ri.order_id=oi.id " +
                "join base_dic for system_time as of ri.pt as dic1 on ri.refund_type=dic1.dic_code " +
                "join base_dic for system_time as of ri.pt as dic2 on ri.refund_reason_type=dic2.dic_code "
        );

//        TODO  7、创建kafka退单事实表映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_ORDER_REFUND+"(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "date_id string," +
                "create_time string," +
                "refund_type_code string," +
                "refund_type_name string," +
                "refund_reason_type_code string," +
                "refund_reason_type_name string," +
                "refund_reason_txt string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint," +
                "primary key(id) not enforced " +
                ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

//        TODO  8、将数据插入到退单事实表映射表去
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);

    }
}
