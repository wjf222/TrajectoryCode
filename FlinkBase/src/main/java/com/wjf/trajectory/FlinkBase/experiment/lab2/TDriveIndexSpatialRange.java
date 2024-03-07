package com.wjf.trajectory.FlinkBase.experiment.lab2;

import com.wjf.trajectory.FlinkBase.operator.range.*;
import com.wjf.trajectory.FlinkBase.operator.similarity.Dataloader;
import com.wjf.trajectory.FlinkBase.operator.util.TextSourceFunction;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import com.wjf.trajectory.common.util.ParamHelper;

public class TDriveIndexSpatialRange {
    public static String dataPath;
    public static String queryPath;
    public static KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Long> rangeMeasure;
    public static String sinkDir;
    private static String measure;
    public static int query_size;
    public static long timeWindowSize;
    public static boolean isIncrement;
    private static XZ2SFC xz2SFC;
    private static  int indexType;
    private static String index;
    private static int dataSize;
    private static long step;
    public static void main(String[] args) throws Exception {
        xz2SFC = new XZ2SFC((short) 10,116.0,116.8,39.5,40.3);
        ParamHelper.initFromArgs(args);
        sinkDir = ParamHelper.getSinkDir();
        dataPath = ParamHelper.getDataPath();
        queryPath = ParamHelper.getQueryPath();
        query_size = ParamHelper.getQuerySize();
        timeWindowSize = ParamHelper.getTimeWindowSize();
        indexType = ParamHelper.getIndexType();
        dataSize = ParamHelper.getDataSize();
        step = ParamHelper.getTimeStep();
        int range_measure_op = ParamHelper.getRangeMeasure();
        switch (range_measure_op) {
            case 1:
                measure = "Origin";
                isIncrement = false;
                break;
            case 2:
                measure = "increment";
                isIncrement = true;
                break;
            default:
                throw new RuntimeException("No Such range Method");
        }

        switch (indexType){
            case 1:
                index = "xz2SFC";
                rangeMeasure = new XZIndexRangeQuery(query_size,timeWindowSize,isIncrement,xz2SFC, (int) step);
                break;
            case 2:
                index = "rtree";
                rangeMeasure = new RTreeStateIndexRangeQuery(query_size,timeWindowSize,dataSize, (int) step);
                break;
            default:
                throw new RuntimeException("No Such index Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        env.getConfig().enableObjectReuse();
        new TDriveIndexSpatialRange().apply(env);
    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .process(rangeMeasure)
                .name("生成范围查询");
        rangeQueryPairStream.addSink(new RangeResultSink(sinkDir));
        env.execute(String.format("TrajectoryCode %s %s Range Query", measure,index));
    }
}
