package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import entity.TracingPoint;
import entity.TracingQueue;
import entity.WindowPoint;
import indexs.IndexRange;
import indexs.commons.Window;
import indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * out的三个输出：
 * Window：查询窗口
 * Boolean:是否包含
 * Long：查询处理时长
 */
public class IndexRangeQuery extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Long> {
    private ValueState<TracingQueue> trajectoryState;
    private ValueStateDescriptor<TracingQueue> trajectoryStateDescriptor;
    private MapStateDescriptor<Window,Integer> windowStateDescriptor;
    private MapState<Window,Integer> windowCounts;
    private MapState<Window,Boolean> windowContain;
    private int query_size;
    private long timeWindowSize;
    private boolean increment;
    private Map<Window,List<IndexRange>> windowIndexRangeMap;
    private XZ2SFC xz2SFC;
    public IndexRangeQuery(int query_size, long timeWindowSize, boolean increment, XZ2SFC xz2SFC) {
        this.query_size = query_size;
        this.timeWindowSize = timeWindowSize;
        this.increment = increment;
        this.xz2SFC = xz2SFC;
        this.windowIndexRangeMap = new HashMap<>();
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
        MapStateDescriptor<Window,Boolean> windowContainDescriptor = new MapStateDescriptor<>(
                "windowContainDescriptor",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.BOOLEAN_TYPE_INFO
        );
        windowContain = getRuntimeContext().getMapState(windowContainDescriptor);
        windowStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Long>.ReadOnlyContext ctx, Collector<Long> out) throws Exception {
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

        TracingPoint first = trajectory.queueArray.getFirst();
        TracingPoint last = trajectory.queueArray.getLast();
        double xMin = Math.min(first.x,last.x);
        double yMin = Math.min(first.y,last.y);
        double xMax = Math.max(first.x,last.x);
        double yMax = Math.max(first.y,last.y);
        for(Map.Entry<Window,Integer> windowIntegerEntry:windows.immutableEntries()) {
            Window window = windowIntegerEntry.getKey();
            if(windowCounts.contains(window)&&windowCounts.get(window) >= windowIntegerEntry.getValue()){
                continue;
            }
            if (!windowIndexRangeMap.containsKey(window)){
                List<Window> windowList = new ArrayList<>();
                windowList.add(window);
                List<IndexRange> ranges = xz2SFC.ranges(windowList, Optional.empty());
                windowIndexRangeMap.put(window,ranges);
            }
            long startTime = System.currentTimeMillis();
            int count = 0;
            if(windowCounts.contains(window)) {
                count = windowCounts.get(window);
            }
            count++;

            windowCounts.put(window,count);
            boolean contain = false;
            boolean preContain = false;
            if(windowContain.contains(window)){
                preContain = windowContain.get(window);
            }
            // 执行轨迹范围查询
            if(!increment) {
                List<IndexRange> ranges = windowIndexRangeMap.get(window);
                long index = xz2SFC.index(xMin, yMin, xMax, yMax, true);
                for (IndexRange range:ranges) {
                    if(range.intersect(index)) {
                        contain = true;
                        // 已经和一个范围相交,不需要继续判断
                        break;
                    }
                }
            }else {
                if(count == 1) {
                    for (TracingPoint tracingPoint : trajectory.queueArray) {
                        if (isPointInsidePolygon(tracingPoint, window.getPointList())) {
                            contain = true;
                        }
                    }
                } else {
                    if(preContain||isPointInsidePolygon(trajectory.queueArray.getLast(), window.getPointList())){
                        contain = true;
                    }
                }
            }
            windowContain.put(window,contain);
            long endTime = System.currentTimeMillis();
            out.collect(endTime-startTime);
        }
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window,Long>.Context ctx, Collector<Long> out) throws Exception {
        BroadcastState<Window, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(window,10);
    }

    public static boolean isPointInsidePolygon(TracingPoint point, List<WindowPoint> polygon) {
        int intersectCount = 0;
        for (int i = 0; i < polygon.size(); i++) {
            WindowPoint p1 = polygon.get(i);
            WindowPoint p2 = polygon.get((i + 1) % polygon.size());

            // 检查水平射线是否与边相交
            if (((p1.getY() > point.y) != (p2.getY()> point.y)) &&
                    (point.x < (p2.getX() - p1.getX()) * (point.y - p1.getY()) / (p2.getY() - p1.getY()) + p1.getX())) {
                intersectCount++;
            }
        }

        // 奇数次相交意味着点在多边形内
        return (intersectCount % 2 == 1);
    }
}
