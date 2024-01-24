package com.wjf.trajectory.FlinkBase.operator.range;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class XZRangeQueryProcess extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Window,List<Long>>> {
    private MapState<Window, List<IndexRange>> windowRangeMap;
    private MapState<Window,Set<Long>> rangeTrajectoryState;
    private transient Counter calculateStrength;
    private MapState<Long, TracingQueue> idTrajectoryMap;
    private MapStateDescriptor<Long,TracingQueue>  idTrajectoryMapStateDescriptor;
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
        idTrajectoryMapStateDescriptor= new MapStateDescriptor<>(
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

        String taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
        // Access Flink's MetricGroup
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        // Create a separate MetricGroup for the function
        MetricGroup functionMetricGroup = metricGroup.addGroup("keyedFunction");

        // Register a new Counter metric for the function
        calculateStrength = functionMetricGroup.counter("customCounter");
        System.out.printf("Metric:%s\tSubTask:%s\r\n",calculateStrength.toString(),getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Window,List<Long>>>.ReadOnlyContext ctx, Collector<Tuple2<Window,List<Long>>> out) throws Exception {
        TracingQueue trajectory = idTrajectoryMap.get(point.id);
        if(trajectory == null) {
            System.out.printf("Metric:%s\tpointId:%d\trest:%d\tshardKey:%d\r\n",calculateStrength.toString(),point.id,point.id%8,point.shardKey);
            trajectory = new TracingQueue();
        }
        trajectory.EnCircularQueue(point);
        trajectory.updateIndex(xz2sfc);
        idTrajectoryMap.put(point.getId(),trajectory);

    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Window,List<Long>>>.Context ctx, Collector<Tuple2<Window,List<Long>>> out) throws Exception {
        List<Window> list = Collections.singletonList(window);
        List<IndexRange> ranges = this.xz2sfc.ranges(list, Optional.empty());
        ctx.applyToKeyedState(idTrajectoryMapStateDescriptor,new KeyedStateFunction<Long, MapState<Long, TracingQueue>>(){
            @Override
            public void process(Long aLong, MapState<Long, TracingQueue> longTracingQueueMapState) throws Exception {
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
                out.collect(Tuple2.of(window,result));
            }
        });
    }
}
