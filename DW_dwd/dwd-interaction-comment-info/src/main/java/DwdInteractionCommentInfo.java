import Gmall_fs.base.BaseSQLApp;
import Gmall_fs.constant.Constant;
import Gmall_fs.util.FlinkSQlUtil;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args)  {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
//        创建一个kafka映射表, 读取业务数据
        FlinkSQlUtil.readOdsdata(tEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

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
        tEnv.executeSql("create table base_dic (" +
                                    " dic_code string," +  // 如果字段是原子类型,则表示这个是 rowKey, 字段随意, 字段类型随意
                                    " info row<dic_name string>, " +  // 字段名和 hbase 中的列族名保持一致. 类型必须是 row. 嵌套进去的就是列
                                    " primary key (dic_code) not enforced " + // 只能用 rowKey 做主键
                                    ")\n"+FlinkSQlUtil.getHbaseDDLSource("dim_base_dic")
        );

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
                            "ts bigint " +
                            ")"+FlinkSQlUtil.getUpsetKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

//        将下沉完的评论表数据输出到kafka映射表上
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

    }
}