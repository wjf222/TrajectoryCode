package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.wjf.trajectory.common.entity.QueryPair;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
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

public class RTreeQueryPairGenerator extends KeyedBroadcastProcessFunction<Long, TracingPoint,QueryTraInfo , QueryPair> {

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
    private ValueState<RTree<TracingPoint, Rectangle>> rtreeState;
    private ValueStateDescriptor<RTree<TracingPoint, Rectangle>> rTreeDescriptor;
    private Map<QueryTraInfo,List<IndexRange>> windowIndexRangeMap;
    public RTreeQueryPairGenerator(long timeWindow, int continuousQueryNum, long step, Similarity incrementSimilarity, XZ2SFC xz2SFC) {
        this.timeWindow = timeWindow;
        this.continuousQueryNum = continuousQueryNum;
        this.step = step;
        this.incrementSimilarity = incrementSimilarity;
        this.xz2SFC = xz2SFC;
        this.windowIndexRangeMap = new HashMap<>();
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        rTreeDescriptor =
                new ValueStateDescriptor<RTree<TracingPoint, Rectangle>>(
                        "rtree",
                        TypeInformation.of(new TypeHint<RTree<TracingPoint, Rectangle>>() {}),
                        RTree.create()
                );
        rtreeState = getRuntimeContext().getState(rTreeDescriptor);
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
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, QueryTraInfo, QueryPair>.ReadOnlyContext ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.value();
        if(tra == null) {
            tra = new TracingQueue(timeWindow,step);
            tra.updateId(point.id);
        }
        tra.EnCircularQueue(point);
        int value = queryNum.value();
        traState.update(tra);
        RTree<TracingPoint, Rectangle> rTree = rtreeState.value();
        if(value%2==0) {
            rTree = addTrajectoryPoint(rTree, point.getX(), point.getY(), point);
            rtreeState.update(rTree);

        }
        if(tra.queueArray.size() < timeWindow){
            return;
        }
        value++;
        queryNum.update(value);

        ReadOnlyBroadcastState<QueryTraInfo, Integer> windows = ctx.getBroadcastState(windowStateDescriptor);
        for(Map.Entry<QueryTraInfo,Integer> windowIntegerEntry:windows.immutableEntries()) {

            QueryTraInfo queryInfo = windowIntegerEntry.getKey();

            if (windowCounts.contains(queryInfo) && windowCounts.get(queryInfo) >= windowIntegerEntry.getValue()) {
                continue;
            }
            QueryPair pair = new QueryPair();

            pair.setStartTimestamp(System.nanoTime());
            int count = 0;
            if(windowCounts.contains(queryInfo)) {
                count = windowCounts.get(queryInfo);
            }
            count++;

            windowCounts.put(queryInfo,count);

            TracingQueue queryTra = queryInfo.queryTra;
            TracingQueue anotherTra = tra;
            boolean identity = queryTra.id == anotherTra.id;
            boolean contain = false;
            double a = queryTra.getXMax()-queryTra.getXMin();
            double b = queryTra.getYMax()-queryTra.getYMin();
            Rectangle queryRectangle = Geometries.rectangle(queryTra.getXMin()-a*1.5, queryTra.getYMin()-b*1.5, queryTra.getXMax()+a*1.5,queryTra.getYMax()+b*1.5);
            List<Long> longs = rangeQuery(rTree, queryRectangle);
            if(longs.size() == 0){
                continue;
            }
            if(count == 1){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }else if(expiration == 0||count%expiration!=0){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra, (int) step);
            }else if(count%expiration==0){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }
            if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;
            windowContain.put(queryInfo,contain);
            pair.setEndTimestamp(System.nanoTime());
            pair.setQueryTraId(queryTra.id);
            out.collect(pair);
        }
    }

    @Override
    public void processBroadcastElement(QueryTraInfo info, KeyedBroadcastProcessFunction<Long, TracingPoint, QueryTraInfo, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        BroadcastState<QueryTraInfo, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(info, (int) continuousQueryNum);
    }
    public static RTree<TracingPoint, Rectangle> addTrajectoryPoint(RTree<TracingPoint, Rectangle> rTree, double x, double y, TracingPoint userData) {
        return rTree.add(userData, Geometries.rectangle(x, y, x, y));
    }

    public static List<Long> rangeQuery(RTree<TracingPoint, Rectangle> rTree, Rectangle queryRectangle){
        return rTree.search(queryRectangle)
                .map(entry -> entry.value().getId()).toList().toBlocking().single();
    }
}
