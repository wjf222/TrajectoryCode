package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import entity.TracingPoint;
import entity.TracingQueue;
import indexs.commons.Window;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * out的三个输出：
 * Window：查询窗口
 * Boolean:是否包含
 * Long：查询处理时长
 */
public class RangeQueryPairGenerator extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, RangeQueryPair> {
    private ValueState<TracingQueue> trajectoryState;
    private ValueStateDescriptor<TracingQueue> trajectoryStateDescriptor;
    private MapStateDescriptor<Window,Integer> windowStateDescriptor;
    private MapState<Window,Integer> windowCounts;
    private int query_size;
    private long timeWindowSize;
    public RangeQueryPairGenerator(int query_size, long timeWindowSize) {
        this.query_size = query_size;
        this.timeWindowSize = timeWindowSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        trajectoryStateDescriptor= new ValueStateDescriptor<>(
                "trajectory",
                TypeInformation.of(new TypeHint<TracingQueue>() {
                })
        );
        trajectoryState = getRuntimeContext().getState(trajectoryStateDescriptor);
        MapStateDescriptor<Window,Integer> windowRangeTrajectoryDescriptor = new MapStateDescriptor<Window,Integer>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        windowCounts = getRuntimeContext().getMapState(windowRangeTrajectoryDescriptor);
        windowStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, RangeQueryPair>.ReadOnlyContext ctx, Collector<RangeQueryPair> out) throws Exception {
        TracingQueue trajectory = trajectoryState.value();
        if(trajectory == null) {
            trajectory = new TracingQueue(timeWindowSize);
            trajectory.updateId(point.id);
        }
        trajectory.EnCircularQueue(point);
        trajectoryState.update(trajectory);
        if(trajectory.queueArray.size() < query_size){
            return;
        }
        ReadOnlyBroadcastState<Window, Integer> windows = ctx.getBroadcastState(windowStateDescriptor);
        for(Map.Entry<Window,Integer> windowIntegerEntry:windows.immutableEntries()) {

            long startTime = System.currentTimeMillis();
            boolean contain = false;
            Window window = windowIntegerEntry.getKey();
            if(windowCounts.contains(window)&&windowCounts.get(window) >= windowIntegerEntry.getValue()){
                continue;
            }
            int count = 0;
            if(windowCounts.contains(window)) {
                count = windowCounts.get(window);
            }
            count++;
            windowCounts.put(window,count);
            RangeQueryPair rangeQueryPair = new RangeQueryPair();
            rangeQueryPair.setTracingQueue(trajectory);
            rangeQueryPair.setWindow(window);
            out.collect(rangeQueryPair);
        }
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window,RangeQueryPair>.Context ctx, Collector<RangeQueryPair> out) throws Exception {
        BroadcastState<Window, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(window,10);
    }


}
