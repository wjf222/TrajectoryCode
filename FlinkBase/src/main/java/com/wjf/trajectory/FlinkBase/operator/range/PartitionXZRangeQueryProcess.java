package com.wjf.trajectory.FlinkBase.operator.range;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.Window;
import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class PartitionXZRangeQueryProcess extends BroadcastProcessFunction<TracingPoint, Window, Tuple2<Window,List<Long>>> {
    private transient Counter calculateStrength;
    private Map<Long, TracingQueue> idTrajectoryMap;
    private final XZ2SFC xz2sfc;

    public PartitionXZRangeQueryProcess(XZ2SFC xz2SFC) {
        this.xz2sfc = xz2SFC;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        idTrajectoryMap = new HashMap<>();
        // Access Flink's MetricGroup
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
        // Create a separate MetricGroup for the function
        MetricGroup functionMetricGroup = metricGroup.addGroup("Trajectory-Range");

        // Register a new Counter metric for the function
        calculateStrength = functionMetricGroup.counter(String.format("customCounter-%s",getRuntimeContext().getIndexOfThisSubtask()));
        System.out.printf("Metric:%s\tSubTask:%s\r\n",calculateStrength.toString(),getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(TracingPoint point, BroadcastProcessFunction<TracingPoint, Window, Tuple2<Window, List<Long>>>.ReadOnlyContext ctx, Collector<Tuple2<Window, List<Long>>> out) throws Exception {
        TracingQueue trajectory = idTrajectoryMap.get(point.id);
        if(trajectory == null) {
            System.out.printf("Metric:%s\tSubTaskId:%d\trest:%d\tshardKey:%d\r\n",calculateStrength.toString(),getRuntimeContext().getIndexOfThisSubtask(),point.id%8,point.shardKey);
            trajectory = new TracingQueue();
        }
        trajectory.EnCircularQueue(point);
        trajectory.updateIndex(xz2sfc);
        idTrajectoryMap.put(point.getId(),trajectory);
    }

    @Override
    public void processBroadcastElement(Window window, BroadcastProcessFunction<TracingPoint, Window, Tuple2<Window, List<Long>>>.Context ctx, Collector<Tuple2<Window, List<Long>>> out) throws Exception {
        List<Window> list = Collections.singletonList(window);
        List<IndexRange> ranges = this.xz2sfc.ranges(list, Optional.empty());
        List<Long> result = new ArrayList<>();
        for(Map.Entry<Long, TracingQueue> trajectoryEntry: idTrajectoryMap.entrySet()) {
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
}
