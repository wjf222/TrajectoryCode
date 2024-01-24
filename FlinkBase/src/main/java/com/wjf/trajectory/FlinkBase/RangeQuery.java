package com.wjf.trajectory.FlinkBase;

import com.wjf.trajectory.FlinkBase.operator.similarity.Dataloader;
import com.wjf.trajectory.FlinkBase.operator.range.RTreeRangeQuery;
import com.wjf.trajectory.FlinkBase.operator.range.RangeInfoLoader;
import com.wjf.trajectory.FlinkBase.operator.range.XZRangeQueryProcess;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import com.wjf.trajectory.common.util.ParamHelper;

import java.util.List;

public class RangeQuery {
    public static String dataPath;
    public static String queryPath;
    public static KeyedBroadcastProcessFunction<Long,TracingPoint, Window, Tuple2<Window, List<Long>>> rangeMeasure;
    public static String sinkDir;
    public static void main(String[] args) throws Exception {
        XZ2SFC xz2SFC = new XZ2SFC((short) 10,116.0,116.8,39.5,40.3);
        ParamHelper.initFromArgs(args);
        sinkDir = ParamHelper.getSinkDir();
        dataPath = ParamHelper.getDataPath();
        queryPath = ParamHelper.getQueryPath();
        int range_measure_op = ParamHelper.getRangeMeasure();
        switch (range_measure_op) {
            case 1:
                rangeMeasure = new XZRangeQueryProcess(xz2SFC);break;
            case 2:
                rangeMeasure = new RTreeRangeQuery(); break;
            default:
                throw new RuntimeException("No Such Similarity Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        new RangeQuery().apply(env);
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
        // 读取query 字符串
        BroadcastStream<Window> queryWindowStream = env
                .readTextFile(queryPath)
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
        SingleOutputStreamOperator<Tuple2<Window,List<Long>>> rangeQueryResultStream = pointStream.keyBy(point -> point.id)
                .connect(queryWindowStream)
                .process(rangeMeasure)
                .name("执行范围查询");
//        rangeQueryResultStream.addSink(new RangeResultSink(sinkDir));
        env.execute("TrajectoryCode Flink Base Range Query Test");
    }
}
