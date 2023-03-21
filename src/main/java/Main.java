import objects.TracingPoint;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Objects;

public class Main {
    private static final String input_topic_name = "trajectory";
    private static final String bootStrapServers = "192.168.245.217:9092";
    private static final String GroupName = "wjf";
    private static final String SourceName = "TrajectorySource";
    private static final int WindowSize = 5;
    private static final int SlideStep = 1;
    public static void main(String[] args) throws Exception {
        // 默认时间语义
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String filepath = "";
        if(args.length >= 1){
            filepath = args[0];
        }
        DataStream<String> stream = textStream(env,filepath);
        DataStream<TracingPoint> tracingPointStream = stream.map(str -> {
            String[] s = str.split(",");
            TracingPoint tracingPoint = TracingPoint.builder()
                    .id(Integer.parseInt(s[0]))
                    .date(ft.parse(s[1]).getTime())
                    .longitude(Double.parseDouble(s[2]))
                    .latitude(Double.parseDouble(s[3])).build();
            return tracingPoint;
        });
        tracingPointStream.print();
//        SingleOutputStreamOperator<TracingPoint> apply = tracingPointStream.keyBy((KeySelector<TracingPoint, String>) tracingPoint -> GeoHash.getBinary(tracingPoint.getLongitude(), tracingPoint.getLatitude(), 13)).window(SlidingProcessingTimeWindows.of(Time.seconds(WindowSize), Time.seconds(SlideStep)))
//                .apply(new WindowFunction<TracingPoint, TracingPoint, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<TracingPoint> iterable, Collector<TracingPoint> collector) throws Exception {
//                        for (TracingPoint point : iterable) {
//                            collector.collect(point);
//                        }
//                    }
//                });
//        apply.print();

        env.execute("Wjf Flink");
    }

    public static DataStream<String> textStream(StreamExecutionEnvironment env,String filpath) {
        if(Objects.equals(filpath, "")) {
            filpath = "D:\\opensource\\wjf\\Trajectory\\TrajectorySystem\\src\\main\\resources\\taxi_log_2008_by_id\\1.txt";
        }
        DataStreamSource<String> stream = env.readTextFile(filpath);
        return stream;
    }

    public static DataStream<String> kafkaStream(StreamExecutionEnvironment env) {
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics(input_topic_name)
                .setGroupId(GroupName)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> Stream = env.fromSource(source, WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofHours(1))
                .withTimestampAssigner((event,timestamp) -> Long.parseLong(event.split(",")[1])), SourceName);
        return Stream;
    }
}
