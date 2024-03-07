package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.wjf.trajectory.common.entity.*;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import com.wjf.trajectory.common.service.Similarity;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class XZQueryPairGenerator extends KeyedBroadcastProcessFunction<Long, TracingPoint,QueryTraInfo , QueryPair> {

    public long timeWindow;
    public int continuousQueryNum;
    public Similarity incrementSimilarity;
    private MapState<QueryTraInfo,Integer> windowCounts;
    //缓存query来之前的tra
    private ValueState<TracingQueue> traState;
    private ValueState<QueryTraInfo> queryInfoValueState;
    private ValueState<Integer> queryNum;
    private MapState<QueryTraInfo,Boolean> windowContain;
    private MapStateDescriptor<QueryTraInfo,Integer> windowStateDescriptor;
    private long step;
    private int expiration;
    private XZ2SFC xz2SFC;
    private int nums;
    private int taskIndex;
    private Map<QueryTraInfo,List<IndexRange>> windowIndexRangeMap;
    public XZQueryPairGenerator(long timeWindow, int continuousQueryNum, long step, Similarity incrementSimilarity,XZ2SFC xz2SFC) {
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
        traState = getRuntimeContext().getState(
                new ValueStateDescriptor<TracingQueue>("traState", TracingQueue.class, new TracingQueue(timeWindow,step))
        );
        queryInfoValueState = getRuntimeContext()
                .getState(new ValueStateDescriptor<QueryTraInfo>("queryInfoValueState",QueryTraInfo.class,new QueryTraInfo()));
        queryNum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("queryNum",Integer.class,new Integer(0)));
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
        windowCounts = getRuntimeContext().getMapState(windowRangeTrajectoryDescriptor);
        MapStateDescriptor<QueryTraInfo,Boolean> windowContainDescriptor = new MapStateDescriptor<>(
                "windowContainDescriptor",
                TypeInformation.of(new TypeHint<QueryTraInfo>() {
                }),
                BasicTypeInfo.BOOLEAN_TYPE_INFO
        );
        windowContain = getRuntimeContext().getMapState(windowContainDescriptor);
        taskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, QueryTraInfo, QueryPair>.ReadOnlyContext ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.value();
        if(tra == null) {
            tra = new TracingQueue(timeWindow,step);
            tra.updateId(point.id);
        }
//        double xMin = tra.getHistoryXmin();
//        double yMin = tra.getHistoryyMin();
//        double xMax = tra.getHistoryxMax();
//        double yMax = tra.getHistoryyMax();
//        double a = xMax-xMin;
//        double b = yMax-yMin;
        int ret = tra.EnCircularQueue(point);
        if(ret == 1) {
            nums++;
            if(nums%100==0)
            System.out.println(taskIndex + "\t:" + nums);
        }
//        if(point.x > xMax+a*0|| point.y > yMax+b*0 || point.x < xMin-a*0 || point.y < yMin-b*0){
//            tra.setHistoryxMax(tra.getXMax());
//            tra.setHistoryXmin(tra.getXMin());
//            tra.setHistoryyMax(tra.getYMax());
//            tra.setHistoryyMin(tra.getYMin());
//            tra.setOut(false);
//        }
        traState.update(tra);
//        if(tra.queueArray.size() < timeWindow){
//            return;
//        }

//        long index = xz2SFC.index(xMin-0*a, yMin-0*b, xMax+0*a, yMax+0*b, true);
//        ReadOnlyBroadcastState<QueryTraInfo, Integer> windows = ctx.getBroadcastState(windowStateDescriptor);
//        for(Map.Entry<QueryTraInfo,Integer> windowIntegerEntry:windows.immutableEntries()) {
//
//            QueryTraInfo queryInfo = windowIntegerEntry.getKey();
//
//            if (windowCounts.contains(queryInfo) && windowCounts.get(queryInfo) >= windowIntegerEntry.getValue()) {
//                continue;
//            }
//            if (!windowIndexRangeMap.containsKey(queryInfo)){
//                List<Window> windowList = new ArrayList<>();
//                TracingPoint firstQuery = queryInfo.queryTra.queueArray.getFirst();
//                TracingPoint lastQuery = queryInfo.queryTra.queueArray.getLast();
//                double xMinQuery = Math.min(firstQuery.x,lastQuery.x);
//                double yMinQuery = Math.min(firstQuery.y,lastQuery.y);
//                double xMaxQuery = Math.max(firstQuery.x,lastQuery.x);
//                double yMaxQuery = Math.max(firstQuery.y,lastQuery.y);
//                windowList.add(new Window(xMinQuery,yMinQuery,xMaxQuery,yMaxQuery));
//                List<IndexRange> ranges = xz2SFC.ranges(windowList, Optional.empty());
//                windowIndexRangeMap.put(queryInfo,ranges);
//            }
//            QueryPair pair = new QueryPair();
//
//            pair.setStartTimestamp(System.nanoTime());
//            int count = 0;
//            if(windowCounts.contains(queryInfo)) {
//                count = windowCounts.get(queryInfo);
//            }
//            count++;
//
//            windowCounts.put(queryInfo,count);
//
//            TracingQueue queryTra = queryInfo.queryTra;
//            TracingQueue anotherTra = tra;
//            boolean identity = queryTra.id == anotherTra.id;
//            boolean contain = false;
//            boolean preContain = false;
//            if(windowContain.contains(queryInfo)){
//                preContain = windowContain.get(queryInfo);
//            }
////            if(count == 1){
//            List<IndexRange> ranges = windowIndexRangeMap.get(queryInfo);
//            int low = 0;
//            int high = ranges.size()-1;
//            while (low <= high && (low <= ranges.size()-1)&&(high <= ranges.size()-1)) {
//                int middle  = (high+low) >> 1;
//                IndexRange midIndex = ranges.get(middle);
//                if(midIndex.intersect(index)) {
//                    contain = true;
//                    // 已经和一个范围相交,不需要继续判断
//                    break;
//                } else if(index < midIndex.lower){
//                    high = middle-1;
//                }else {
//                    low = middle+1;
//                }
//            }
//            if(contain&&tra.isOut()){
//                tra.setOut(false);
//                nums++;
//                tra.setHistoryxMax(tra.getXMax());
//                tra.setHistoryXmin(tra.getXMin());
//                tra.setHistoryyMax(tra.getYMax());
//                tra.setHistoryyMin(tra.getYMin());
//                tra.setOut(false);
//                nums++;
//                System.out.println(taskIndex+"\t:"+nums);
//
//            }
//                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
//            }else if(expiration == 0||count%expiration!=0){
//                if(preContain)
//                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra, (int) step);
//            }else if(count%expiration==0){
//                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
//            }
//            if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;
//            windowContain.put(queryInfo,contain);
//            pair.setEndTimestamp(System.nanoTime());
//            pair.setQueryTraId(queryTra.id);
//            out.collect(pair);
//        }
    }

    @Override
    public void processBroadcastElement(QueryTraInfo info, KeyedBroadcastProcessFunction<Long, TracingPoint, QueryTraInfo, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        BroadcastState<QueryTraInfo, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(info, (int) continuousQueryNum);
    }
}
