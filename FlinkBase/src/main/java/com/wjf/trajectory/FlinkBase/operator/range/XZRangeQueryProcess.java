package com.wjf.trajectory.FlinkBase.operator.range;

import entity.TracingPoint;
import entity.TracingQueue;
import indexs.IndexRange;
import indexs.commons.Window;
import indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class XZRangeQueryProcess extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, String> {
    private MapState<Window, List<IndexRange>> windowRangeMap;
    private MapState<Window,Set<Long>> rangeTrajectoryState;
    private MapState<Long, TracingQueue> idTrajectoryMap;
    private final XZ2SFC xz2sfc;

    public XZRangeQueryProcess(XZ2SFC xz2SFC) {
        this.xz2sfc = xz2SFC;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<Window,List<IndexRange>> windowListMapStateDescriptor = new MapStateDescriptor<>(
                "WindowCount",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                TypeInformation.of(new TypeHint<List<IndexRange>>() {
                })
        );
        windowRangeMap = getRuntimeContext().getMapState(windowListMapStateDescriptor);
        MapStateDescriptor<Long,TracingQueue>  idTrajectoryMapStateDescriptor= new MapStateDescriptor<>(
                "WindowCount",
                BasicTypeInfo.LONG_TYPE_INFO,
                TypeInformation.of(new TypeHint<TracingQueue>() {
                })
        );
        idTrajectoryMap = getRuntimeContext().getMapState(idTrajectoryMapStateDescriptor);
        MapStateDescriptor<Window,Set<Long>> windowRangeTrajectoryDescriptor = new MapStateDescriptor<Window, Set<Long>>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                TypeInformation.of(new TypeHint<Set<Long>>() {
                })
        );
        rangeTrajectoryState = getRuntimeContext().getMapState(windowRangeTrajectoryDescriptor);
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        TracingQueue trajectory = idTrajectoryMap.get(point.id);
        trajectory.EnCircularQueue(point);
        trajectory.updateIndex(xz2sfc);
        idTrajectoryMap.put(point.getId(),trajectory);
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, String>.Context ctx, Collector<String> out) throws Exception {
        List<Window> list = Collections.singletonList(window);
        List<IndexRange> ranges = this.xz2sfc.ranges(list, Optional.empty());
        windowRangeMap.put(window, ranges);
        List<Long> result = new ArrayList<>();
        for(Map.Entry<Long, TracingQueue> trajectoryEntry: idTrajectoryMap.entries()) {
            for (IndexRange range:ranges) {
                if(range.intersect(trajectoryEntry.getValue().getIndex())) {
                    result.add(trajectoryEntry.getKey());
                    // 已经和一个范围相交,不需要继续判断
                    break;
                }
            }
        }
        String record = result.stream()
                .distinct()
                .sorted()
                .map(Objects::toString)
                .collect(Collectors.joining(", "));
        out.collect(record);
    }
}
