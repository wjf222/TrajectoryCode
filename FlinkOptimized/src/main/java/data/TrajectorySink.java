package data;

import conf.KafkaProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class TrajectorySink {
    public static Sink<String> createKafkaSink(){
        return KafkaSink.<String>builder()
                .setBootstrapServers(KafkaProperties.BOOT_STRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(KafkaProperties.SINK_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
