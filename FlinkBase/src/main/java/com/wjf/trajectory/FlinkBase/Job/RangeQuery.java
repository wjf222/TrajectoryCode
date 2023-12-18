package com.wjf.trajectory.FlinkBase.Job;

import com.wjf.trajectory.FlinkBase.operator.Dataloader;
import com.wjf.trajectory.FlinkBase.operator.range.RangeInfoLoader;
import com.wjf.trajectory.FlinkBase.operator.range.XZRangeQueryProcess;
import entity.TracingPoint;
import indexs.commons.Window;
import indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import service.Similarity;
import service.similarity.*;
import util.ParamHelper;

public class RangeQuery {
    public static String dataPath;
    public static String queryPath;
    public static int dataSize;
    public static int continuousQueryNum;
    public static Similarity distMeasure;
    public static double lcssThr;
    public static int lcssDelta;
    public static double edrThr;
    public static TracingPoint erpGap;
    public static long delayReduceTime;
    public static String sinkDir;
    public static long timeWindowSize;
    public static void main(String[] args) throws Exception {
        ParamHelper.initFromArgs(args);
        sinkDir = ParamHelper.getSinkDir();
        dataPath = ParamHelper.getDataPath();
        queryPath = ParamHelper.getQueryPath();
        dataSize = ParamHelper.getDataSize();
        continuousQueryNum = ParamHelper.getContinuousQueryNum();
        lcssThr = ParamHelper.getLCSSThreshold();
        lcssDelta = ParamHelper.getLCSSDelta();
        edrThr = ParamHelper.getEDRThreshold();
        erpGap = ParamHelper.getERPGap();
        delayReduceTime = ParamHelper.getDelayReduceTime();
        timeWindowSize = ParamHelper.getTimeWindowSize();
        int dist_measure_op = ParamHelper.getDistMeasure();
        switch (dist_measure_op) {
            case 1:
                distMeasure = new DTW();break;
            case 2:
                distMeasure = new LCSS(lcssThr, lcssDelta); break;
            case 3:
                distMeasure = new EDR(edrThr); break;
            case 4:
                distMeasure = new ERP(erpGap); break;
            case 12:
                distMeasure = new InLCSS();break;
            default:
                throw new RuntimeException("No Such Similarity Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        new RangeQuery().apply(env);

    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }

    public void apply(StreamExecutionEnvironment env) throws Exception {
        XZ2SFC xz2SFC = new XZ2SFC((short) 10,116.0,116.8,39.5,40.3);
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
        pointStream.keyBy(point -> point.id)
                .connect(queryWindowStream)
                .process(new XZRangeQueryProcess(xz2SFC));
    }
}
