package com.wjf.trajectory.FlinkBase.operator.range;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import entity.TracingPoint;
import entity.TracingQueue;
import indexs.commons.Window;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RTreeRangeQuery extends KeyedBroadcastProcessFunction<Long,TracingPoint, Window,String> {
    private ValueState<RTree<TracingPoint, Rectangle>> rtreeState;
    public ValueState<Rectangle> mbrState;

    private MapState<Long, TracingQueue> trajectoryState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<RTree<TracingPoint, Rectangle>> descriptor = new ValueStateDescriptor<RTree<TracingPoint, Rectangle>>(
                "rtree",
                TypeInformation.of(new TypeHint<RTree<TracingPoint, Rectangle>>() {}),
                RTree.create()
                );
        rtreeState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        long trajectoryID = point.getId();
        TracingQueue trajectory = trajectoryState.get(trajectoryID);
        if(trajectory == null) {
            trajectory = new TracingQueue();
        }
        trajectory.EnCircularQueue(point);
        RTree<TracingPoint, Rectangle> rTree = rtreeState.value();
        rTree = addTrajectoryPoint(rTree,point.getLongitude(),point.getLatitude(),point);
        rtreeState.update(rTree);
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, String>.Context ctx, Collector<String> out) throws Exception {
        RTree<TracingPoint, Rectangle> rtree = rtreeState.value();
        Rectangle queryRectangle = Geometries.rectangle(window.getXmin(), window.getYmin(), window.getXmax(), window.getYmax());
        List<Long> result = rangeQuery(rtree,queryRectangle);
        // 结果去重
        String record = result.stream()
                .distinct()
                .sorted()
                .map(Objects::toString)
                .collect(Collectors.joining(", "));
        out.collect(record);
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
