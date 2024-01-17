package com.wjf.trajectory.FlinkBase.experiment.lab1;

import com.wjf.trajectory.FlinkBase.operator.Dataloader;
import com.wjf.trajectory.FlinkBase.operator.range.OriginRangeQuery;
import com.wjf.trajectory.FlinkBase.operator.range.RangeInfoLoader;
import com.wjf.trajectory.FlinkBase.operator.range.RangeResultSink;
import com.wjf.trajectory.FlinkBase.operator.util.TextSourceFunction;
import entity.TracingPoint;
import indexs.commons.Window;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import util.ParamHelper;

import java.util.List;

public class TDriveSpatialRange {
    public static String dataPath;
    public static String queryPath;
    public static KeyedBroadcastProcessFunction<Long,TracingPoint, Window, Tuple3<Window, Boolean,Long>> rangeMeasure;
    public static String sinkDir;
    private static String measure;
    public static void main(String[] args) throws Exception {
        ParamHelper.initFromArgs(args);
        sinkDir = ParamHelper.getSinkDir();
        dataPath = ParamHelper.getDataPath();
        queryPath = ParamHelper.getQueryPath();
        int range_measure_op = ParamHelper.getRangeMeasure();
        switch (range_measure_op) {
            case 1:
                measure = "Origin";
                rangeMeasure = new OriginRangeQuery(); break;
            default:
                throw new RuntimeException("No Such range Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        new TDriveSpatialRange().apply(env);
    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }

    public void apply(StreamExecutionEnvironment env) throws Exception {
        MapStateDescriptor<String,Window> windowMapStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Window>() {})
        );
        ;
        // 读取query 字符串
        BroadcastStream<Window> queryWindowStream = env
                .addSource(new TextSourceFunction(queryPath))
                .keyBy(queryline -> 1)
                .process(new RangeInfoLoader())
                .broadcast(windowMapStateDescriptor);
        // 并行读取Point 流
        SingleOutputStreamOperator<TracingPoint> pointStream = env
                .readTextFile(dataPath)
                // 分发轨迹流到不同节点
                .keyBy(line -> Long.parseLong(line.split(",")[0]))
                .flatMap(new Dataloader())
                .name("轨迹数据文件读入");
        // 开始时间戳
//        pointStream = pointStream
//                .connect(queryWindowStream)
//                .process()
//                .map(pair -> {
//                    pair.startTimestamp = System.currentTimeMillis();
//                    return pair;
//                })
//                .name("开始时间戳");
        SingleOutputStreamOperator<Tuple3<Window, Boolean,Long>> rangeQueryResultStream = pointStream
                .keyBy(point ->point.id)
                .connect(queryWindowStream)
                .process(rangeMeasure)
                .name("执行范围查询");
        rangeQueryResultStream.addSink(new RangeResultSink(sinkDir));
        env.execute(String.format("TrajectoryCode %s Range Query", measure));
    }
}
