import com.wjf.trajectory.common.partition.QueryPairKeySelector;
import com.wjf.trajectory.common.entity.QueryInfo;
import com.wjf.trajectory.common.entity.QueryPair;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.service.Similarity;
import operator.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import com.wjf.trajectory.common.service.similarity.DTW;
import com.wjf.trajectory.common.service.similarity.EDR;
import com.wjf.trajectory.common.service.similarity.ERP;
import com.wjf.trajectory.common.service.similarity.LCSS;
import com.wjf.trajectory.common.util.ParamHelper;


public class Main {
    public static String dataPath;
    public static String queryPath;
    public static int dataSize;
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

        lcssThr = ParamHelper.getLCSSThreshold();
        lcssDelta = ParamHelper.getLCSSDelta();
        edrThr = ParamHelper.getEDRThreshold();
        erpGap = ParamHelper.getERPGap();
        delayReduceTime = ParamHelper.getDelayReduceTime();
        timeWindowSize = ParamHelper.getTimeWindowSize();
        int dist_measure_op = ParamHelper.getDistMeasure();
        if (dist_measure_op == 1) {
            distMeasure = new DTW();
        } else if (dist_measure_op == 2) {
            distMeasure = new LCSS(lcssThr, lcssDelta);
        } else if (dist_measure_op == 3) {
            distMeasure = new EDR(edrThr);
        } else {
            distMeasure = new ERP(erpGap);
        }
        // 默认时间语义
        final StreamExecutionEnvironment env = initEnv();
        new Main().apply(env);
        env.execute("TrajectoryCode Flink Optimized");
    }

    public static StreamExecutionEnvironment initEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        return env;
    }
    public void apply(StreamExecutionEnvironment env) {
        // 读取query 字符串
        SingleOutputStreamOperator<QueryInfo> queryInfoStream = env
                .readTextFile(queryPath)
                .keyBy(queryline -> 1)
                .process(new QueryInfoLoader())
                .name("查询字符串输入");
        // 并行读取Point 流
        SingleOutputStreamOperator<TracingPoint> pointStream = env
                .readTextFile(dataPath)
                .keyBy(line -> Long.parseLong(line.split(",")[0]))
                .flatMap(new Dataloader())
                .name("点数据字符串读入");
        // 两流合并获取查询内容
        SingleOutputStreamOperator<QueryTraInfo> queryTraInfoStream = pointStream.connect(queryInfoStream)
                .keyBy(point -> point.id,info -> info.queryTraId)
                .process(new QueryTraInfoGenerator(timeWindowSize))
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
        // 基于XZ2剪枝
        queryPairSingleOutputStreamOperator
                .filter(new XZFilter())
                .keyBy(new ComputedKeySelector());
        // 开始时间戳
        queryPairSingleOutputStreamOperator = queryPairSingleOutputStreamOperator
                .map(pair -> {
                    pair.startTimestamp = System.currentTimeMillis();
                    return pair;
                })
                .name("开始时间戳");
        // 相似度计算
        queryPairSingleOutputStreamOperator
                .keyBy(new QueryPairKeySelector())
                .process(new SimilarCalculator(distMeasure))
                .name("相似度计算");
        // 结束时间戳
        queryPairSingleOutputStreamOperator = queryPairSingleOutputStreamOperator
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
    }
}
