package Gmall_fs.util;

import Gmall_fs.constant.Constant;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQlUtil {
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

    public static String getUpsetKafkaDDLSink(String topic ){
        return "with(" +
                            "  'connector' = 'kafka'," +
                            "  'topic' = '" + topic + "'," +
                            "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                            "  'format' = 'json' " +
                            ")";
    }

    /**
     * 读取业务Ods数据，映射为topic_db动态表
     * @param tEnv  流动表环境
     * @param group_id  消费组id
     */
    public static void readOdsdata(StreamTableEnvironment tEnv, String group_id ){
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

}
