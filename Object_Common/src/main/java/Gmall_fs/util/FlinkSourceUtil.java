package Gmall_fs.util;

import Gmall_fs.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {

    /**
     * 获取kafka连接
     * @param topic
     * @param group_id
     * @return
     */
    public static KafkaSource<String> getkafkaSource(String topic , String group_id){
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(group_id)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }).build();

        return kafkaSource;


    }

    /**
     * 获取mysql连接
     * @param database
     * @param table
     * @return
     */
    public static MySqlSource<String> getMySqlSource(String database, String table ){
        //       4.1    配置mysqlcdc
        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("useSSL", "false");
        jdbcProperties.setProperty("allowPublicKeyRetrieval", "true");
//        4.2   配置mysql设置
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(database)
                .tableList(database+"."+table)     // 切记要带数据库，不然查找不到
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(jdbcProperties)
                .startupOptions(StartupOptions.initial())  // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return mySqlSource;


    }



}
