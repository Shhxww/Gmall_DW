import Gmall_fs.bean.TableProcessDim;
import Gmall_fs.constant.Constant;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.val;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.client.RedirectStrategy;

import java.io.IOException;
import java.util.Properties;

public class DimAPP {
    public static void main(String[] args) throws Exception {
//        TODO 1、 设置初始环境
//        1.1 创建初始环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        1.2 设置程序全局并行度
        env.setParallelism(4);

//        TODO 2、 配置检查点、重启策略
//        2.1   启用检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        2.2   设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        2.3   设置检查点间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        2.4   设置检查点在任务结束后进行保存，及其保存路径(保存在hdfs上要指定有权限的用户)
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setCheckpointStorage("hdfs://node1:8020/Flink_checkpoint/");
//        2.5   设置重启策略（每3s重启一次，30天内仅能重启三次）
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(3),Time.seconds(3)));

//        TODO  3、读取kafka上的业务数据
//        3.1   创建一个kafka对象
        KafkaSource<String> kafkaSource = KafkaSource
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
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
                        return false ;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
//        3.2   封装成流
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaDS");
//        kafkaDS.print();

//        TODO  4、 配置流
//       4.1    配置mysqlcdc
        Properties  jdbcProperties = new Properties();
        jdbcProperties.setProperty("useSSL", "false");
        jdbcProperties.setProperty("allowPublicKeyRetrieval", "true");
//        4.2   配置mysql设置
        MySqlSource<String> gmallConfig = MySqlSource
                .<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.gmall_config")     // 切记要带数据库，不然查找不到
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(jdbcProperties)
                .startupOptions(StartupOptions.initial())  // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
//        4.3   封装成流(并行度要设置成1，不然会导致配置流出现乱序，无法同步)
        DataStreamSource<String> gmall_config = env.fromSource(gmallConfig, WatermarkStrategy.noWatermarks(), "Gmall_config").setParallelism(1);
//         4.4  将读取到的配置数据进行转换
        SingleOutputStreamOperator<TableProcessDim> tpDS = gmall_config.map(
                new RichMapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
//                        将json字符串转换成Json实体类
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
//                        提取出操作类型
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
//                        判断是否为删除操作
                        if (op.equals("d")) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                            return tableProcessDim;
                        } else {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                            return tableProcessDim;
                        }
                    }
                }
        );
        tpDS.print();


//        gmall_config.print();


        env.execute();


    }
}
