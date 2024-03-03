package com.wjf.trajectory.FlinkBase.experiment.lab1;

import com.wjf.trajectory.FlinkBase.operator.range.*;
import com.wjf.trajectory.FlinkBase.operator.similarity.Dataloader;
import com.wjf.trajectory.FlinkBase.operator.util.TextSourceFunction;
import com.wjf.trajectory.common.entity.RangeQueryPair;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.indexs.commons.Window;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import com.wjf.trajectory.common.util.ParamHelper;

public class TDriveSpatialRange {
    public static String dataPath;
    public static String queryPath;
    public static KeyedProcessFunction<Long, RangeQueryPair,RangeQueryPair> rangeMeasure;
    public static String sinkDir;
    private static String measure;
    public static int query_size;
    public static long timeWindowSize;
    public static boolean isIncrement;
    private static long step;
    public static int continuousQueryNum;
    public static void main(String[] args) throws Exception {
        ParamHelper.initFromArgs(args);
        sinkDir = ParamHelper.getSinkDir();
        dataPath = ParamHelper.getDataPath();
        queryPath = ParamHelper.getQueryPath();
        query_size = ParamHelper.getQuerySize();
        continuousQueryNum = ParamHelper.getContinuousQueryNum();
        timeWindowSize = ParamHelper.getTimeWindowSize();
        step = ParamHelper.getTimeStep();
        int range_measure_op = ParamHelper.getRangeMeasure();
        switch (range_measure_op) {
            case 1:
                measure = "Origin";
                isIncrement = false;
                rangeMeasure = new OriginRangeQuery(); break;
            case 2:
                measure = "increment";
                isIncrement = true;
                rangeMeasure = new IncrementRangeQuery();break;
            default:
                throw new RuntimeException("No Such range Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        env.getConfig().enableObjectReuse();
        new TDriveSpatialRange().apply(env);
    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }

    public void apply(StreamExecutionEnvironment env) throws Exception {
        MapStateDescriptor<Window,Integer> windowMapStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
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
        SingleOutputStreamOperator<Long> rangeQueryPairStream = pointStream
                .keyBy(point ->point.id)
                .connect(queryWindowStream)
                .process(new RangeQueryPairGenerator(query_size,timeWindowSize,isIncrement,step,continuousQueryNum))
                .name("生成范围查询");
        rangeQueryPairStream.addSink(new RangeResultSink(sinkDir));
        env.execute(String.format("TrajectoryCode %s Range Query", measure));
    }
}
