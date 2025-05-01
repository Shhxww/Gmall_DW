import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:   互动域--评论事实表（每一条评论）
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-14 08:03:17
 **/

//数据样本：
// {
    // "database":"gmall",
    // "table":"comment_info",
    // "type":"delete",
    // "ts":1746276431,
    // "xid":50391,
    // "xoffset":19,
    // "data":{
//                      "id":1861,
//                      "user_id":3717,
//                      "nick_name":"惠珠",
//                      "head_img":null,
//                      "sku_id":10,
//                      "spu_id":3,
//                      "order_id":19591,
//                      "appraise":"1201",
//                      "comment_txt":"评论内容：86916181228663234643569699182154688159573237275489",
//                      "create_time":"2025-04-26 20:44:46",
//                      "operate_time":null
//                     }
// }

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args)  {
//        启动程序
        new DwdInteractionCommentInfo().start(
                10012,
                4,
                Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        创建一个kafka映射表, 读取业务数据
        FlinkSQlUtil.readOdsData(tEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

//        从Ods数据里过滤出评论表数据
        Table commentInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['appraise'] appraise, " +
                        " `data`['comment_txt'] comment_txt, " +
                        " `data`['create_time'] comment_time," +
                        " ts, " +
                        " pt " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='comment_info' " +
                        " and `type`='insert' "
        );

//        将过滤出来的数据向动态表注册成comment_info表
        tEnv.createTemporaryView("comment_info", commentInfo);

//        读取hbase上的字典维度表数据
        FlinkSQlUtil.readHbaseDic(tEnv);

//        进行look join维度下沉
        Table result = tEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "ci.comment_txt," +
                "ci.ts " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise=dic.dic_code");

//        创建一个kafka映射表
        tEnv.executeSql("create table "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+"(" +
                            "id string, " +
                            "user_id string," +
                            "sku_id string," +
                            "appraise string," +
                            "appraise_name string," +
                            "comment_txt string," +
                            "ts bigint, " +
                            "primary key(id) not enforced " +
                            ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

//        将下沉完的评论表数据输出到kafka映射表上
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

    }
}