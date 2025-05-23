package Gmall_fs.util;

import Gmall_fs.bean.TableProcessDwd;
import Gmall_fs.constant.Constant;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                        .<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(topic+"_")
//                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }


    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
    return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
            .setBootstrapServers(Constant.KAFKA_BROKERS)
            .setRecordSerializer(
//                    自定义序列化器
                    new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                @Nullable
                @Override
                public ProducerRecord<byte[], byte[]> serialize(
                                                                Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
                                                                KafkaSinkContext context,
                                                                Long timestamp) {
                    String topic = dataWithConfig.f1.getSinkTable();
                    JSONObject data = dataWithConfig.f0;
                    data.remove("op_type");
                    return new ProducerRecord<>(topic, data.toJSONString().getBytes(StandardCharsets.UTF_8));
                }
            })
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("B1ue-" + new Random().nextLong())
            .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
            .build();
}

    /**
     * 获取Doris输出类
     * @param table 目标表名
     * @return Doris输出类
     */
    public static DorisSink<String> getDorisSink(String table) {
    Properties props = new Properties();
    props.setProperty("format", "json");
    props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
    return DorisSink.<String>builder()
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                    .setFenodes(Constant.DORIS_FE_NODES)
                    .setTableIdentifier(table)
                    .setUsername("root")
                    .setPassword("000000")
                    .build()
            )
            .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
//                    .setLabelPrefix()  // stream-load 导入数据时 label 的前缀
                    .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                    .setBufferCount(3) // 批次条数: 默认 3
                    .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                    .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                    .setMaxRetries(3)
                    .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                    .build())
            .setSerializer(new SimpleStringSerializer())
            .build();
}

}
