package com.wjf.trajectory.FlinkBase.experiment.lab3;

import com.wjf.trajectory.FlinkBase.operator.partition.*;
import com.wjf.trajectory.FlinkBase.operator.range.RangeInfoLoader;
import com.wjf.trajectory.FlinkBase.operator.util.TextSourceFunction;
import entity.TracingPoint;
import indexs.commons.Window;
import indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import util.ParamHelper;

public class KeyPartitionTDriveIndexSpatialRange {
    public static String dataPath;
    public static String queryPath;
    public static KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Integer,Long>> rangeMeasure;
//    public static BroadcastProcessFunction<TracingPoint, Window, Tuple2<Integer,Long>> rangeMeasure;
    public static String sinkDir;
    private static String measure;
    public static int query_size;
    public static long timeWindowSize;
    public static boolean isIncrement;
    private static XZ2SFC xz2SFC;
    private static  int indexType;
    private static String index;
    private static String host;
    private static String port;
    private static int dataSize;
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
        host = ParamHelper.getJobManagerHost();
        port = ParamHelper.getJobManagerPort();
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
                rangeMeasure = new KeyPartitionXZIndexRangeQuery(query_size,timeWindowSize,isIncrement,xz2SFC);
                break;
            default:
                throw new RuntimeException("No Such index Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        env.getConfig().enableObjectReuse();
        new KeyPartitionTDriveIndexSpatialRange().apply(env);
    }

    public static StreamExecutionEnvironment initEnv() {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,8081);
////        configuration.set(RestOptions.ADDRESS,"127.0.0.1");
////        configuration.set(RestOptions.BIND_PORT,"8081");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .process(new Dataloader(host,port))
                .name("轨迹数据文件读入");
        SingleOutputStreamOperator<Tuple2<Integer,Long>> rangeQueryPairStream = pointStream
                .partitionCustom(new CustomPartitioner<>(),new CustomKeySelector())
                .connect(queryWindowStream)
                .process(rangeMeasure)
                .name("PartitionRangeQuery");
        rangeQueryPairStream.addSink(new PartitionRangeResultSink(sinkDir));
        env.execute(String.format("TrajectoryCode %s %s Range Query", measure,index));
        System.out.print(1);
    }
}
