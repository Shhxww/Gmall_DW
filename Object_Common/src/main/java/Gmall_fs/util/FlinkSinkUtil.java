package Gmall_fs.util;

import Gmall_fs.bean.TableProcessDwd;
import Gmall_fs.constant.Constant;
import com.alibaba.fastjson.JSONObject;
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
import java.util.Random;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink
                .<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema
                        .<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix(topic+"_")
//                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
    return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
            .setBootstrapServers(Constant.KAFKA_BROKERS)
            .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                @Nullable
                @Override
                public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
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

}
