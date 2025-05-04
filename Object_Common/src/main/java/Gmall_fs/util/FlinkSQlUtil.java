package Gmall_fs.util;

import Gmall_fs.constant.Constant;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQlUtil {
    /**
     * 获取kafka连接器
     * @param topic 主题
     * @param groupId   消费组名
     * @return
     */
    public static String getKafkaDDLSource(String topic , String groupId ){
        return "with(" +
                            "  'connector' = 'kafka'," +
                            "  'properties.group.id' = '" + groupId + "'," +
                            "  'topic' = '" + topic + "'," +
                            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                            "  'scan.startup.mode' = 'latest-offset'," +
                            "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                            "  'format' = 'json' " +
            ")";
    }

    /**
     * 获取HBase连接器
     * @param tablename 目标表名
     * @return
     */
    public static String getHbaseDDLSource(String tablename ){
        return  "WITH (" +
                                " 'connector' = 'hbase-2.2'," +
                                " 'table-name' = '"+Constant.HBASE_NAMESPACE+":"+tablename+"',"+
                                " 'zookeeper.quorum' = 'node1:2181,node2:2181,node3:2181', " +
                                " 'lookup.cache' = 'PARTIAL', " +
                                " 'lookup.async' = 'true', " +
                                " 'lookup.partial-cache.max-rows' = '20', " +
                                " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                                ")";
    }

    /**
     * 获取upsert-kafka连接器
     * @param topic 主题
     * @return
     */
    public static String getUpsetKafkaDDLSink(String topic ){
        return "with(" +
                            "  'connector' = 'upsert-kafka'," +
                            "  'topic' = '" + topic + "'," +
                            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                            "  'key.json.ignore-parse-errors' = 'true'," +
                            "  'value.json.ignore-parse-errors' = 'true'," +
                            "  'key.format' = 'json', " +
                            "  'value.format' = 'json' " +
                            ")";
    }

    /**
     * 读取业务Ods数据，映射为topic_db动态表
     * @param tEnv  流动表环境
     * @param group_id  消费组id
     */
    public static void readOdsData(StreamTableEnvironment tEnv, String group_id ){
        tEnv.executeSql("create table topic_db (" +
                            "  `database` string, " +
                            "  `table` string, " +
                            "  `type` string, " +
                            "  `data` map<string, string>, " +
                            "  `old` map<string, string>, " +
                            "  `ts` bigint, " +
                            "  `pt` as proctime(), " +
                            "  et as to_timestamp_ltz(ts, 0), " +
                            "  watermark for et as et - interval '3' second " +
                            ") "+ FlinkSQlUtil.getKafkaDDLSource(Constant.TOPIC_DB, group_id)
        );
    }

    /**
     * 读取HBase上的字典维度表数据
     * @param tEnv
     */
    public static void readHbaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table base_dic (" +
                                    " dic_code string," +  // 如果字段是原子类型,则表示这个是 rowKey, 字段随意, 字段类型随意
                                    " info row<dic_name string>, " +  // 字段名和 hbase 中的列族名保持一致. 类型必须是 row. 嵌套进去的就是列
                                    " primary key (dic_code) not enforced " + // 只能用 rowKey 做主键
                                    ")\n"+FlinkSQlUtil.getHbaseDDLSource("dim_base_dic")
        );
    }

}
