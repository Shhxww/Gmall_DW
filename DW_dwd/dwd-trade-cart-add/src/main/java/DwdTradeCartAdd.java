import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 * 需启动的服务：zk、Kafka、mysql、maxwell、主程序
 */

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        TODO  读取kafka上的业务数据，映射为topic_db表
        FlinkSQlUtil.readOdsData(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
//        TODO  从业务数据中过滤出加购数据
//        过滤出来
        Table cart_info = tEnv.sqlQuery("select " +
                " `data`['id'] id," +
                " `data`['user_id'] user_id," +
                " `data`['sku_id'] sku_id," +
                " if(`type`='insert'," +
                "   cast(`data`['sku_num'] as int), " +
                "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                ") sku_num ," +
                " ts " +
                "from topic_db " +
                "where `table`='cart_info' " +
                "and (" +
                " `type`='insert' " +
                "  or(" +
                "     `type`='update' " +
                "      and `old`['sku_num'] is not null " +
                "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                "   )" +
                ")");
//        向动态表环境注册
        tEnv.createTemporaryView("cart_info", cart_info);
//        TODO  创建kafka加购事实表的映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(" +
                            "   id string, " +
                            "   user_id string," +
                            "   sku_id string," +
                            "   sku_num int, " +
                            "   ts  bigint " +
                            ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

//        TODO  将过滤出来的数据插入到映射表中去
        cart_info.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
