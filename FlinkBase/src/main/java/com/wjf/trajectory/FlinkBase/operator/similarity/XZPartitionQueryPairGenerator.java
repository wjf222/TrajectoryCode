package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.wjf.trajectory.common.entity.QueryPair;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import com.wjf.trajectory.common.service.Similarity;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XZPartitionQueryPairGenerator extends BroadcastProcessFunction<TracingPoint,QueryTraInfo , QueryPair> {

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
    public void processElement(TracingPoint value, BroadcastProcessFunction<TracingPoint, QueryTraInfo, QueryPair>.ReadOnlyContext ctx, Collector<QueryPair> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(QueryTraInfo info, BroadcastProcessFunction<TracingPoint, QueryTraInfo, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        BroadcastState<QueryTraInfo, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(info, continuousQueryNum);
    }
}
