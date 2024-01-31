package com.wjf.trajectory.FlinkBase.operator.partition;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.entity.WindowPoint;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import com.wjf.trajectory.common.util.math.Tools;

import java.util.*;

/**
 * out的三个输出：
 * Window：查询窗口
 * Boolean:是否包含
 * Long：查询处理时长
 */
public class CustomPartitionXZIndexRangeQuery extends BroadcastProcessFunction<TracingPoint, Window, Tuple2<Integer,Long>> {
    private Map<Long,TracingQueue> trajectoryState;
    private MapStateDescriptor<Window,Integer> windowStateDescriptor;
    private Map<Window,Map<Long,Integer>> windowCounts;
    private Map<Window,Boolean> windowContain;
    private int query_size;
    private long timeWindowSize;
    private boolean increment;
    private Map<Window,List<IndexRange>> windowIndexRangeMap;
    private XZ2SFC xz2SFC;
    private transient Counter pointsCounter;
    private transient Counter calTimeCounter;
    public CustomPartitionXZIndexRangeQuery(int query_size, long timeWindowSize, boolean increment, XZ2SFC xz2SFC) {
        this.query_size = query_size;
        this.timeWindowSize = timeWindowSize;
        this.increment = increment;
        this.xz2SFC = xz2SFC;
        this.windowIndexRangeMap = new HashMap<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        trajectoryState = new HashMap<>();
        MapStateDescriptor<Window,Integer> windowRangeTrajectoryDescriptor = new MapStateDescriptor<Window,Integer>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        windowCounts = new HashMap<>();
        windowContain = new HashMap<>();
        windowStateDescriptor = new MapStateDescriptor<>(
                "windowState",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                BasicTypeInfo.INT_TYPE_INFO
        );
        this.pointsCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("CustomPartition")
                .counter("PointsCounter");
        this.calTimeCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("CustomPartition")
                .counter("CalTimeCounter");
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

    @Override
    public void processElement(TracingPoint point, BroadcastProcessFunction<TracingPoint, Window, Tuple2<Integer, Long>>.ReadOnlyContext ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
        TracingQueue trajectory = trajectoryState.get(point.getId());
        if(trajectory == null) {
            trajectory = new TracingQueue(timeWindowSize);
            trajectory.updateId(point.id);
        }
        trajectory.EnCircularQueue(point);
        trajectoryState.put(trajectory.id,trajectory);
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
        long index = xz2SFC.index(xMin, yMin, xMax, yMax, true);
        for(Map.Entry<Window,Integer> windowIntegerEntry:windows.immutableEntries()) {
            Window window = windowIntegerEntry.getKey();
            if(windowCounts.containsKey(window)&&windowCounts.get(window).getOrDefault(point.getId(),0) >= windowIntegerEntry.getValue()){
                continue;
            }
            if (!windowIndexRangeMap.containsKey(window)){
                List<Window> windowList = new ArrayList<>();
                windowList.add(window);
                List<IndexRange> ranges = xz2SFC.ranges(windowList, Optional.empty());
                windowIndexRangeMap.put(window,ranges);
            }
            Map<Long,Integer> trajectoryWindowCount;
            if(windowCounts.containsKey(window)){
                trajectoryWindowCount = windowCounts.get(window);
            } else {
                trajectoryWindowCount = new HashMap<>();
            }
            long startTime = Tools.currentMicrosecond();
            int count = trajectoryWindowCount.getOrDefault(point.getId(),0);
            count++;
            trajectoryWindowCount.put(point.getId(),count);
            windowCounts.put(window,trajectoryWindowCount);
            boolean contain = false;
            boolean preContain = false;
            if(windowContain.containsKey(window)){
                preContain = windowContain.get(window);
            }
            // 执行轨迹范围查询
            if(!increment) {
                List<IndexRange> ranges = windowIndexRangeMap.get(window);
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
            }else {
                if(count == 1) {
                    List<IndexRange> ranges = windowIndexRangeMap.get(window);
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
                } else {
                    if(preContain||isPointInsidePolygon(trajectory.queueArray.getLast(), window.getPointList())){
                        contain = true;
                    }
                }
            }
            windowContain.put(window,contain);
            long endTime = Tools.currentMicrosecond();
            this.pointsCounter.inc();
            this.calTimeCounter.inc(endTime-startTime);
            out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(),endTime-startTime));
        }
    }

    @Override
    public void processBroadcastElement(Window window, BroadcastProcessFunction<TracingPoint, Window, Tuple2<Integer, Long>>.Context ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
        BroadcastState<Window, Integer> broadcastState = ctx.getBroadcastState(windowStateDescriptor);
        broadcastState.put(window,10);
    }
}
