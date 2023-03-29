import indexs.GeoHash;
import objects.TracingPoint;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import service.DTW;
import service.LCSS;
import service.Similarity;
import util.Pretreatment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class Main {
    private static final String input_topic_name = "trajectory";
    private static final String bootStrapServers = "192.168.245.217:9092";
    private static final String GroupName = "wjf";
    private static final String SourceName = "TrajectorySource";
    private static final int WindowSize = 1;
    private static final int SlideStep = 15;

    public static void main(String[] args) throws Exception {
        // 默认时间语义
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String filepath = "";
        if (args.length >= 1) {
            filepath = args[0];
        }
        DataStream<String> stream = textStream(env, filepath).assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (str, l) -> {
                            String[] s = str.split(",");
                            try {
                                return ft.parse(s[1]).getTime();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            return 0;
                        })
        );
        DataStream<TracingPoint> tracingPointStream = stream.map(str -> {
            String[] s = str.split(",");
            TracingPoint tracingPoint = TracingPoint.builder()
                    .id(Integer.parseInt(s[0]))
                    .date(ft.parse(s[1]).getTime())
                    .longitude(Double.parseDouble(s[2]))
                    .latitude(Double.parseDouble(s[3])).build();
            return tracingPoint;
        });
        TracingPoint[] source = new TracingPoint[5];
        source[0] = TracingPoint.builder().id(0).longitude(116.28149).latitude(39.91596).build();
        source[1] = TracingPoint.builder().id(0).longitude(116.27739).latitude(39.91267).build();
        source[2] = TracingPoint.builder().id(0).longitude(116.27712).latitude(39.91684).build();
        source[3] = TracingPoint.builder().id(0).longitude(116.28909).latitude(39.91758).build();
        source[4] = TracingPoint.builder().id(0).longitude(116.29604).latitude(39.91197).build();
        SingleOutputStreamOperator<Double> apply = tracingPointStream
                .filter((FilterFunction<TracingPoint>) value -> {
                    try {
                        Pretreatment.positionRange(value);
                    } catch (Exception e){
                        e.printStackTrace();
                        return false;
                    }
                    return true;
                })
                .keyBy((KeySelector<TracingPoint, String>) tracingPoint -> GeoHash.getBinary(tracingPoint.getLongitude(), tracingPoint.getLatitude(), 13))
                .window(SlidingEventTimeWindows.of(Time.hours(WindowSize), Time.minutes(SlideStep)))
                .apply(new WindowFunction<TracingPoint, Double, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TracingPoint> iterable, Collector<Double> collector) {
                        Similarity op = new LCSS();
                        Map<Integer, ArrayList<TracingPoint>> map = new HashMap<>();
                        for(TracingPoint t:iterable){
                            ArrayList<TracingPoint> traj = map.getOrDefault(t.id,new ArrayList<>());
                            traj.add(t);
                            map.put(t.id,traj);
                        }
                        for(ArrayList<TracingPoint> trajectory:map.values()){
                            double val = op.compute(trajectory.toArray(new TracingPoint[0]),source);
                            op.compute(trajectory.toArray(new TracingPoint[0]),source);
                            op.compute(trajectory.toArray(new TracingPoint[0]),source);
                            op.compute(trajectory.toArray(new TracingPoint[0]),source);
                            op.compute(trajectory.toArray(new TracingPoint[0]),source);
                            collector.collect(val);
                        }
                    }
                }).setParallelism(8);
        apply.print().setParallelism(8);

        env.execute("Wjf Flink");
    }

    public static DataStream<String> textStream(StreamExecutionEnvironment env, String filpath) {
        if (Objects.equals(filpath, "")) {
            filpath = "D:\\wjf\\graduatestudent\\TrajectoryCode\\src\\main\\resources\\data-sorted.txt";
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
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[1])), SourceName);
        return Stream;
    }
}
