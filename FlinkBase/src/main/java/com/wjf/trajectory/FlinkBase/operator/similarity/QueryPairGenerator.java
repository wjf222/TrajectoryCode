package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.wjf.trajectory.common.entity.*;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.service.Similarity;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class QueryPairGenerator extends KeyedBroadcastProcessFunction<Long, TracingPoint,QueryTraInfo , QueryPair> {

    public long timeWindow;
    public int continuousQueryNum;
    public Similarity incrementSimilarity;
    private MapState<QueryTraInfo,Integer> windowCounts;
    //缓存query来之前的tra
    private ValueState<TracingQueue> traState;
    private ValueState<QueryTraInfo> queryInfoValueState;
    private ValueState<Integer> queryNum;
    private MapStateDescriptor<QueryTraInfo,Integer> windowStateDescriptor;
    private long step;
    private int expiration;
    public QueryPairGenerator(long timeWindow,int continuousQueryNum,long step,Similarity incrementSimilarity) {
        this.timeWindow = timeWindow;
        this.continuousQueryNum = continuousQueryNum;
        this.step = step;
        this.incrementSimilarity = incrementSimilarity;
    }
    public QueryPairGenerator(long timeWindow,int continuousQueryNum,long step,Similarity incrementSimilarity,int expiration) {
        this.timeWindow = timeWindow;
        this.continuousQueryNum = continuousQueryNum;
        this.step = step;
        this.incrementSimilarity = incrementSimilarity;
        this.expiration=expiration;
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
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, QueryTraInfo, QueryPair>.ReadOnlyContext ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.value();
        if(tra == null) {
            tra = new TracingQueue(timeWindow,step);
            tra.updateId(point.id);
        }
        tra.EnCircularQueue(point);
        traState.update(tra);
        if(tra.queueArray.size() < timeWindow){
            return;
        }
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
            if(count == 1){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }else if(expiration == 0||count%expiration!=0){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra, (int) step);
            }else if(count%expiration==0){
                pair.similarityDistance = identity ? 0.0 : incrementSimilarity.compute(anotherTra, queryTra,  anotherTra.queueArray.size());
            }
            if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;
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
}
