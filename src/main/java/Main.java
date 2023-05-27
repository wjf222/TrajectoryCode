import data.TrajectorySink;
import indexs.z2.GeoHash;
import objects.TracingPoint;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import service.ClosestPairDistance;
import service.Similarity;
import util.Pretreatment;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static data.TrajectorySource.textStream;

public class Main {
    private static final int WindowSize = 1;
    private static final int SlideStep = 15;
    private static final int Parallelism = 2;
    private static TracingPoint[] source = new TracingPoint[5];
    static {

        source[0] = TracingPoint.builder().id(0).longitude(116.28149).latitude(39.91596).build();
        source[1] = TracingPoint.builder().id(0).longitude(116.27739).latitude(39.91267).build();
        source[2] = TracingPoint.builder().id(0).longitude(116.27712).latitude(39.91684).build();
        source[3] = TracingPoint.builder().id(0).longitude(116.28909).latitude(39.91758).build();
        source[4] = TracingPoint.builder().id(0).longitude(116.29604).latitude(39.91197).build();
    }
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
        }).filter((FilterFunction<TracingPoint>) Pretreatment::positionRange);
        KeyedStream<TracingPoint, String> tracingPointStringKeyedStream = tracingPointStream
                .keyBy((KeySelector<TracingPoint, String>) tracingPoint -> GeoHash.getBinary(tracingPoint.getLongitude(), tracingPoint.getLatitude(), 13));
//        DataStream<String> apply = slidingWindowDeal(tracingPointStringKeyedStream);
        DataStream<String> apply = tumblingWindowDeal(tracingPointStringKeyedStream);
        Sink<String> sink = TrajectorySink.createKafkaSink();
        apply.sinkTo(sink);
        env.execute("Wjf Flink");
    }

    public static DataStream<String> slidingWindowDeal(KeyedStream<TracingPoint,String> keyedStream) {
        return keyedStream.window(SlidingEventTimeWindows.of(Time.hours(WindowSize), Time.minutes(SlideStep)))
                .apply(new WindowFunction<TracingPoint, String, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TracingPoint> iterable, Collector<String> collector) {
                        Similarity op = new ClosestPairDistance();
                        Map<Integer, ArrayList<TracingPoint>> map = new HashMap<>();
                        for(TracingPoint t:iterable){
                            ArrayList<TracingPoint> traj = map.getOrDefault(t.id,new ArrayList<>());
                            traj.add(t);
                            map.put(t.id,traj);
                        }
                        for(ArrayList<TracingPoint> trajectory:map.values()){
                            double val = op.compute(source,trajectory.toArray(new TracingPoint[0]));
                            op.compute(source,trajectory.toArray(new TracingPoint[0]));
                            op.compute(source,trajectory.toArray(new TracingPoint[0]));
                            op.compute(source,trajectory.toArray(new TracingPoint[0]));
                            op.compute(source,trajectory.toArray(new TracingPoint[0]));
                            collector.collect(val+"");
                        }
                    }
                }).setParallelism(Parallelism);
    }

    public static DataStream<String> tumblingWindowDeal(KeyedStream<TracingPoint,String> keyedStream) {
        return keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(SlideStep)))
                .apply(new RichWindowFunction<TracingPoint, String, String, TimeWindow>() {
                    private transient MapState<Integer, ClosestPairDistance> trajectoriesMap;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer, ClosestPairDistance> descriptor = new MapStateDescriptor<>(
                                "", Types.INT, Types.GENERIC(ClosestPairDistance.class));
                        trajectoriesMap = getRuntimeContext().getMapState(descriptor);
                    }
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TracingPoint> input, Collector<String> out) throws Exception {
                        for(TracingPoint t:input){
                            if(!trajectoriesMap.contains(t.id)){
                               trajectoriesMap.put(t.id,new ClosestPairDistance(source));
                            }
                            ClosestPairDistance similarity = trajectoriesMap.get(t.id);
                            similarity.incrementCompute(t);
                            similarity.incrementCompute(t);
                            similarity.incrementCompute(t);
                            similarity.incrementCompute(t);
                            out.collect(similarity.incrementCompute(t)+"");
                        }
                    }
                }).setParallelism(Parallelism);
    }
}
