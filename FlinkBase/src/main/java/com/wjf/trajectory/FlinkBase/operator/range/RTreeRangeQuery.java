package com.wjf.trajectory.FlinkBase.operator.range;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.google.gson.Gson;
import entity.RangeQueryResult;
import entity.TracingPoint;
import entity.TracingQueue;
import indexs.commons.Window;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RTreeRangeQuery extends KeyedBroadcastProcessFunction<Long,TracingPoint, Window, Tuple2<Window,List<Long>>> {
    private ValueState<RTree<TracingPoint, Rectangle>> rtreeState;
    private ValueStateDescriptor<RTree<TracingPoint, Rectangle>> rTreeDescriptor;
    private MapState<Long, TracingQueue> trajectoryState;

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
        MapStateDescriptor<Long, TracingQueue> trajectoryDescriptor= new MapStateDescriptor<Long, TracingQueue>(
                "trajectoryMap",
                BasicTypeInfo.LONG_TYPE_INFO,
                TypeInformation.of(new TypeHint<TracingQueue>() {})
        );
        trajectoryState = getRuntimeContext().getMapState(trajectoryDescriptor);
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Window,List<Long>>>.ReadOnlyContext ctx, Collector<Tuple2<Window,List<Long>>> out) throws Exception {
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
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple2<Window,List<Long>>>.Context ctx, Collector<Tuple2<Window,List<Long>>> out) throws Exception {
        ctx.applyToKeyedState(rTreeDescriptor, new KeyedStateFunction<Long, ValueState<RTree<TracingPoint, Rectangle>>>() {
            @Override
            public void process(Long pointId, ValueState<RTree<TracingPoint, Rectangle>> rTreeValueState) throws Exception {
                RTree<TracingPoint, Rectangle> rtree = rtreeState.value();
                Rectangle queryRectangle = Geometries.rectangle(window.getXmin(), window.getYmin(), window.getXmax(), window.getYmax());
                List<Long> result = rangeQuery(rtree,queryRectangle);
                out.collect(Tuple2.of(window,result));
            }
        });

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
