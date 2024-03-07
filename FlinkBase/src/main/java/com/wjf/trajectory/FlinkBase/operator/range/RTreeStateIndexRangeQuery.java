package com.wjf.trajectory.FlinkBase.operator.range;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.indexs.commons.Window;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class RTreeStateIndexRangeQuery extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Long> {
    private ValueState<TracingQueue> trajectoryState;
    private ValueStateDescriptor<TracingQueue> trajectoryStateDescriptor;
    private MapStateDescriptor<Window,Integer> windowStateDescriptor;
    private int query_size;
    private long timeWindowSize;
    private MapState<Window,Integer> windowCounts;
    private int step;
    private ValueState<Integer> queryNum;
    private ValueState<RTree<TracingPoint, Rectangle>> rtreeState;
    private ValueStateDescriptor<RTree<TracingPoint, Rectangle>> rTreeDescriptor;
    public RTreeStateIndexRangeQuery(int query_size, long timeWindowSize,int dataSize) {
        this.query_size = query_size;
        this.timeWindowSize = timeWindowSize;
    }
    public RTreeStateIndexRangeQuery(int query_size, long timeWindowSize,int dataSize,int step) {
        this.query_size = query_size;
        this.timeWindowSize = timeWindowSize;
        this.step = step;
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
        trajectoryStateDescriptor= new ValueStateDescriptor<>(
                "trajectory",
                TypeInformation.of(new TypeHint<TracingQueue>() {
                })
        );
        trajectoryState = getRuntimeContext().getState(trajectoryStateDescriptor);
        MapStateDescriptor<Window,Boolean> windowContainDescriptor = new MapStateDescriptor<>(
                "windowContainDescriptor",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.BOOLEAN_TYPE_INFO
        );
        MapState<Window, Boolean> windowContain = getRuntimeContext().getMapState(windowContainDescriptor);
        windowStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        MapStateDescriptor<Window,Integer> windowCountsDescriptor = new MapStateDescriptor<Window,Integer>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        windowCounts = getRuntimeContext().getMapState(windowCountsDescriptor);
        queryNum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("queryNum",Integer.class,new Integer(0)));
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Long>.ReadOnlyContext ctx, Collector<Long> out) throws Exception {
        TracingQueue trajectory = trajectoryState.value();
        if(trajectory == null) {
            trajectory = new TracingQueue(timeWindowSize,step);
            trajectory.updateId(point.id);
        }
        trajectory.EnCircularQueue(point);
        trajectoryState.update(trajectory);
        long startTime = System.nanoTime();
        int value = queryNum.value();
        value++;
        queryNum.update(value);
        RTree<TracingPoint, Rectangle> rTree = rtreeState.value();
        if(value % 5 == 0) {
            rTree = addTrajectoryPoint(rTree, point.getX(), point.getY(), point);
            rtreeState.update(rTree);
        }
        ReadOnlyBroadcastState<Window, Integer> windows = ctx.getBroadcastState(windowStateDescriptor);
        for (Map.Entry<Window, Integer> windowIntegerEntry : windows.immutableEntries()) {
            Window window = windowIntegerEntry.getKey();
            if(windowCounts.contains(window)){
                continue;
            }

            Rectangle queryRectangle = Geometries.rectangle(window.getXmin(), window.getYmin(), window.getXmax(), window.getYmax());
            for(int i = 0; i < windowIntegerEntry.getValue();i++) {
                rangeQuery(rTree, queryRectangle);
            }
            windowCounts.put(window,windowIntegerEntry.getValue());
        }
        long endTime = System.nanoTime();
        out.collect(endTime-startTime);
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window,Long>.Context ctx, Collector<Long> out) throws Exception {
        BroadcastState<Window, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(window,10);
    }

    // 范围查询
    public static List<Long> rangeQuery(RTree<TracingPoint, Rectangle> rTree, Rectangle queryRectangle){
        return rTree.search(queryRectangle)
                .map(entry -> entry.value().getId()).toList().toBlocking().single();
    }

    // 添加轨迹点到 RTree
    public static RTree<TracingPoint, Rectangle> addTrajectoryPoint(RTree<TracingPoint, Rectangle> rTree, double x, double y, TracingPoint userData) {
        return rTree.add(userData, Geometries.rectangle(x, y, x, y));
    }
}
