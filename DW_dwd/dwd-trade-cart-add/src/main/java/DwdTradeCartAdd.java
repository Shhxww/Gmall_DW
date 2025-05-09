import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:   交易域--加购事实表（加购的每一件商品）
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-14 09:14:05
 **/

//数据样本：
//    {
//    "database":"gmall",
//    "table":"cart_info",
//    "type":"insert",
//    "ts":1746276281,
//    "xid":47148,
//    "commit":true,
//    "data":{"id":19479,"user_id":"1207","sku_id":6,"cart_price":1299.00,"sku_num":1,"img_url":null,"sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","is_checked":null,"create_time":"2025-04-26 20:44:41","operate_time":null,"is_ordered":0,"order_time":null}}

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {
//        启动程序
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

//        TODO  1、读取kafka上的业务数据，映射为topic_db表
        FlinkSQlUtil.readOdsData(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);

//        TODO  2、从业务数据中过滤出加购数据
//        过滤出来
        Table cart_info = tEnv.sqlQuery(
                "select " +
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

//        TODO  3、创建kafka加购事实表的映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(" +
                            "   id string, " +
                            "   user_id string," +
                            "   sku_id string," +
                            "   sku_num int, " +
                                "ts bigint," +
                                "primary key(id) not enforced " +
                            ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

//        TODO  将过滤出来的数据插入到映射表中去
        cart_info.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
