package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.wjf.trajectory.common.entity.QueryPair;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.math.Tools;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class XZPartitionQueryPairGenerator extends BroadcastProcessFunction<TracingPoint,QueryTraInfo , QueryPair> {

    public long timeWindow;
    public int continuousQueryNum;
    public Similarity incrementSimilarity;
    //缓存query来之前的tra
    private Map<Long,TracingQueue> traState;
    private Map<QueryTraInfo,Map<Long,Integer>> windowCounts;
    private Map<QueryTraInfo,Boolean> windowContain;
    private MapStateDescriptor<QueryTraInfo,Integer> windowStateDescriptor;
    private long step;
    private int expiration;
    private XZ2SFC xz2SFC;
    private int nums;
    private int taskIndex;
    private Map<QueryTraInfo,List<IndexRange>> windowIndexRangeMap;
    private transient Counter pointsCounter;
    private transient Counter calTimeCounter;
    public XZPartitionQueryPairGenerator(long timeWindow, int continuousQueryNum, long step, Similarity incrementSimilarity, XZ2SFC xz2SFC) {
        this.timeWindow = timeWindow;
        this.continuousQueryNum = continuousQueryNum;
        this.step = step;
        this.incrementSimilarity = incrementSimilarity;
        this.xz2SFC = xz2SFC;
        this.windowIndexRangeMap = new HashMap<>();
        this.nums = 0;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = new HashMap<>();
        windowStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<QueryTraInfo>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        MapStateDescriptor<QueryTraInfo,Integer> windowRangeTrajectoryDescriptor = new MapStateDescriptor<QueryTraInfo,Integer>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<QueryTraInfo>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        windowCounts = new HashMap<>();
        windowContain = new HashMap<>();
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
        this.pointsCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("CustomPartition")
                .counter("PointsCounter");
        this.calTimeCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("CustomPartition")
                .counter("CalTimeCounter");
    }


    @Override
    public void processElement(TracingPoint point, BroadcastProcessFunction<TracingPoint, QueryTraInfo, QueryPair>.ReadOnlyContext ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.get(point.id);
        if(tra == null) {
            tra = new TracingQueue(timeWindow,step);
            tra.updateId(point.id);
        }
        tra.EnCircularQueue(point);
        traState.put(tra.id,tra);
        if(tra.queueArray.size() < timeWindow){
            return;
        }
        double xMin = tra.getXMin();
        double yMin = tra.getYMin();
        double xMax = tra.getXMax();
        double yMax = tra.getYMax();
        long index = xz2SFC.index(xMin, yMin, xMax, yMax, true);
        ReadOnlyBroadcastState<QueryTraInfo, Integer> windows = ctx.getBroadcastState(windowStateDescriptor);
        for(Map.Entry<QueryTraInfo,Integer> windowIntegerEntry:windows.immutableEntries()) {
            QueryTraInfo queryInfo = windowIntegerEntry.getKey();
            if (windowCounts.containsKey(queryInfo) && windowCounts.get(queryInfo).getOrDefault(point.getId(),0) >= windowIntegerEntry.getValue()) {
                continue;
            }
            if (!windowIndexRangeMap.containsKey(queryInfo)){
                List<Window> windowList = new ArrayList<>();
                double xMinQuery = queryInfo.queryTra.getXMin();
                double yMinQuery = queryInfo.queryTra.getYMin();
                double xMaxQuery = queryInfo.queryTra.getXMax();
                double yMaxQuery = queryInfo.queryTra.getYMax();
                windowList.add(new Window(xMinQuery,yMinQuery,xMaxQuery,yMaxQuery));
                List<IndexRange> ranges = xz2SFC.ranges(windowList, Optional.empty());
                windowIndexRangeMap.put(queryInfo,ranges);
            }
            QueryPair pair = new QueryPair();

            Map<Long,Integer> trajectoryWindowCount;
            if(windowCounts.containsKey(queryInfo)){
                trajectoryWindowCount = windowCounts.get(queryInfo);
            } else {
                trajectoryWindowCount = new HashMap<>();
            }
            long startTime = Tools.currentMicrosecond();
            pair.setStartTimestamp(startTime*1000+getRuntimeContext().getIndexOfThisSubtask());
            int count = trajectoryWindowCount.getOrDefault(point.getId(),0);
            count++;
            trajectoryWindowCount.put(point.getId(),count);
            windowCounts.put(queryInfo,trajectoryWindowCount);

            TracingQueue queryTra = queryInfo.queryTra;
            TracingQueue anotherTra = tra;
            boolean identity = queryTra.id == anotherTra.id;
            boolean contain = false;
            boolean preContain = false;
            if(windowContain.containsKey(queryInfo)){
                preContain = windowContain.get(queryInfo);
            }
            if(count == 1){
                List<IndexRange> ranges = windowIndexRangeMap.get(queryInfo);
                int low = 0;
                int high = ranges.size()-1;
                while (low <= high && (low <= ranges.size()-1)&&(high <= ranges.size()-1)) {
                    int middle  = (high+low) >> 1;
                    IndexRange midIndex = ranges.get(middle);
                    if(midIndex.intersect(index)) {
                        contain = true;
                        // 已经和一个范围相交,不需要继续判断
                        break;
                    } else if(index < midIndex.lower){
                        high = middle-1;
                    }else {
                        low = middle+1;
                    }
                }
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }else if(expiration == 0||count%expiration!=0){
                if(preContain)
                    pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra, (int) step);
            }else if(count%expiration==0){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }
            if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;
            windowContain.put(queryInfo,contain);
            long endTime = Tools.currentMicrosecond();
            pair.setEndTimestamp(endTime*1000);
            pair.setQueryTraId(queryTra.id);
            this.pointsCounter.inc();
            this.calTimeCounter.inc(endTime-startTime);
            out.collect(pair);
        }
    }

    @Override
    public void processBroadcastElement(QueryTraInfo info, BroadcastProcessFunction<TracingPoint, QueryTraInfo, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        BroadcastState<QueryTraInfo, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(info, continuousQueryNum);
    }
}
