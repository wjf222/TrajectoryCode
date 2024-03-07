package com.wjf.trajectory.FlinkBase.experiment.lab3;

import com.wjf.trajectory.FlinkBase.operator.partition.CustomKeySelector;
import com.wjf.trajectory.FlinkBase.operator.partition.CustomPartitioner;
import com.wjf.trajectory.FlinkBase.operator.similarity.*;
import com.wjf.trajectory.common.entity.QueryInfo;
import com.wjf.trajectory.common.entity.QueryPair;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.service.similarity.*;
import com.wjf.trajectory.common.util.ParamHelper;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomePartitionXZTDriveIndexSimilarityTest {
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
    public static int query_size;
    private static long step;
    private static int expiration;
    private static XZ2SFC xz2SFC;
    public static void main(String[] args) throws Exception {
        xz2SFC = new XZ2SFC((short) 10,116.0,116.8,39.5,40.3);
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
        query_size = ParamHelper.getQuerySize();
        step = ParamHelper.getTimeStep();
        expiration = ParamHelper.getExpiration();
        int dist_measure_op = ParamHelper.getDistMeasure();
        switch (dist_measure_op) {
            case 1:
                distMeasure = new DTW();break;
            case 2:
                distMeasure = new LCSS(lcssThr, lcssDelta); break;
            case 3:
                distMeasure = new ClosestPairDistance(); break;
            case 4:
                distMeasure = new EDR(edrThr); break;
            case 5:
                distMeasure = new ERP(erpGap); break;
            case 11:
                distMeasure = new InDTW();break;
            case 12:
                distMeasure = new InLCSS();break;
            case 13:
                distMeasure = new InClosestPairDistance();break;
            default:
                throw new RuntimeException("No Such Similarity Method");
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        new CustomePartitionXZTDriveIndexSimilarityTest().apply(env);
    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }

    public void apply(StreamExecutionEnvironment env) throws Exception {
        MapStateDescriptor<QueryTraInfo,Integer> windowMapStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<QueryTraInfo>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        // 读取query 字符串
        SingleOutputStreamOperator<QueryInfo> queryInfoStream = env
                .readTextFile(queryPath)
                .keyBy(queryline -> 1)
                .process(new QueryInfoLoader())
                .name("查询字符串输入");
        // 并行读取Point 流
        SingleOutputStreamOperator<TracingPoint> pointStream = env
                .readTextFile(dataPath)
                // 分发轨迹流到不同节点
                .keyBy(line -> Long.parseLong(line.split(",")[0]))
                .flatMap(new Dataloader())
                .name("轨迹数据文件读入");
        // 两流合并获取查询内容
        BroadcastStream<QueryTraInfo> queryTraInfoStream = pointStream.connect(queryInfoStream)
                .keyBy(point -> point.id,info -> info.queryTraId)
                .process(new QueryTraInfoGenerator(timeWindowSize,continuousQueryNum,query_size,step))
                .broadcast(windowMapStateDescriptor);
        SingleOutputStreamOperator<QueryPair> queryPairSingleOutputStreamOperator = pointStream
                .partitionCustom(new CustomPartitioner<>(),new CustomKeySelector())
                .connect(queryTraInfoStream)
                .process(new XZQueryPairGenerator(timeWindowSize,continuousQueryNum,step,distMeasure,xz2SFC))
                .name("广播数据结合点信息相似度计算");
        //写入文件
        queryPairSingleOutputStreamOperator.addSink(new ResultToFileSinker(sinkDir));
        env.execute("TrajectoryCode Flink Base Test");
    }
}
