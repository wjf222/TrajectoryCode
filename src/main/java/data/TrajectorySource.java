package data;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Objects;

public class TrajectorySource {
    private static final String input_topic_name = "trajectory";
    private static final String bootStrapServers = "172.27.18.117:9092";
    private static final String GroupName = "wjf";
    private static final String SourceName = "TrajectorySource";
    public static DataStream<String> kafkaStream(StreamExecutionEnvironment env) {
        // 设置事件时间
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics(input_topic_name)
                .setGroupId(GroupName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> Stream = env.fromSource(source, WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofHours(1))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[1])), SourceName);
        return Stream;
    }
}
