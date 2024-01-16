package com.wjf.trajectory.FlinkBase;

import Partition.QueryPairKeySelector;
import com.wjf.trajectory.FlinkBase.operator.*;
import com.wjf.trajectory.FlinkBase.operator.job.SimilarCalculator;
import entity.QueryInfo;
import entity.QueryPair;
import entity.QueryTraInfo;
import entity.TracingPoint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import service.Similarity;
import service.similarity.*;
import util.ParamHelper;

public class Main {
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
        query_size = ParamHelper.getQuerySize();
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
        new Main().apply(env);

    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }

    public void apply(StreamExecutionEnvironment env) throws Exception {
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
        SingleOutputStreamOperator<QueryTraInfo> queryTraInfoStream = pointStream.connect(queryInfoStream)
                .keyBy(point -> point.id,info -> info.queryTraId)
                .process(new QueryTraInfoGenerator(timeWindowSize,continuousQueryNum,query_size))
                .name("两流合并获取查询内容");
        // 结合Point,生成计算对象
        SingleOutputStreamOperator<QueryTraInfo> broadcastQueryTraInfoStream = queryTraInfoStream
                .keyBy(queryTraInfo -> queryTraInfo.info.queryTraId)
                .flatMap(new QueryTraInfoBroadcaster(dataSize))
                .name("广播数据");
        SingleOutputStreamOperator<QueryPair> queryPairSingleOutputStreamOperator = broadcastQueryTraInfoStream.connect(pointStream)
                .keyBy(queryTraInfo -> queryTraInfo.anotherTraId, point -> point.id)
                .process(new QueryPairGenerator(timeWindowSize))
                .name("广播数据结合点信息");
        // 开始时间戳
        queryPairSingleOutputStreamOperator = queryPairSingleOutputStreamOperator
                .map(pair -> {
                    pair.startTimestamp = System.currentTimeMillis();
                    return pair;
                })
                .name("开始时间戳");
        // 相似度计算
        SingleOutputStreamOperator<QueryPair> similarOutputStream = queryPairSingleOutputStreamOperator
                .keyBy(new QueryPairKeySelector())
                .process(new SimilarCalculator(distMeasure))
                .name("相似度计算");
        // 结束时间戳
        queryPairSingleOutputStreamOperator = similarOutputStream
                .map(pair -> {
                    pair.endTimestamp = System.currentTimeMillis();
                    return pair;
                })
                .name("结束时间戳");
        // 统计时间戳
        final OutputTag<QueryPair> lateReduceQueryPair = new OutputTag<QueryPair>("lateReduceQueryPair") {
        };
        // 生成计算结果
        SingleOutputStreamOperator<QueryPair> reduceQueryPairStream = queryPairSingleOutputStreamOperator
                .keyBy(pair -> pair.queryTra.id)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(delayReduceTime)))
                .sideOutputLateData(lateReduceQueryPair)
                .reduce(new ResultReducer());
        SingleOutputStreamOperator<QueryPair> resultQueryPairStream = reduceQueryPairStream.connect(reduceQueryPairStream.getSideOutput(lateReduceQueryPair))
                .keyBy(pair -> pair.queryTra.id, pair -> pair.queryTra.id)
                .map(new CoMapFunction<QueryPair, QueryPair, QueryPair>() {
                    @Override
                    public QueryPair map1(QueryPair value) throws Exception {
                        return value;
                    }

                    @Override
                    public QueryPair map2(QueryPair value) throws Exception {
                        return value;
                    }
                })
                .keyBy(pair -> pair.queryTra.id)
                .reduce(new ResultReducer());
        //写入文件
        resultQueryPairStream.addSink(new ResultToFileSinker(sinkDir));
        env.execute("TrajectoryCode Flink Base Test");
    }
}
