package com.wjf.trajectory.FlinkBase.operator.range;

import entity.TracingPoint;
import entity.TracingQueue;
import indexs.commons.Window;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * out的三个输出：
 * Window：查询窗口
 * Boolean:是否包含
 * Long：查询处理时长
 */
public class OriginRangeQuery extends KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple3<Window, Boolean,Long>> {
    private ValueState<TracingQueue> trajectoryState;
    private ValueStateDescriptor<TracingQueue> trajectoryStateDescriptor;

    public OriginRangeQuery() {
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
        MapStateDescriptor<Window,Set<Long>> windowRangeTrajectoryDescriptor = new MapStateDescriptor<Window, Set<Long>>(
                "WindowRangeTrajectory",
                TypeInformation.of(new TypeHint<Window>() {
                }),
                TypeInformation.of(new TypeHint<Set<Long>>() {
                })
        );
    }

    @Override
    public void processElement(TracingPoint point, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple3<Window, Boolean,Long>>.ReadOnlyContext ctx, Collector<Tuple3<Window, Boolean,Long>> out) throws Exception {
        TracingQueue trajectory = trajectoryState.value();
        if(trajectory == null) {
            trajectory = new TracingQueue();
        }
        trajectory.EnCircularQueue(point);
        trajectoryState.update(trajectory);
    }

    @Override
    public void processBroadcastElement(Window window, KeyedBroadcastProcessFunction<Long, TracingPoint, Window, Tuple3<Window, Boolean,Long>>.Context ctx, Collector<Tuple3<Window, Boolean,Long>> out) throws Exception {
        ctx.applyToKeyedState(trajectoryStateDescriptor,new KeyedStateFunction<Long, ValueState<TracingQueue>>(){
            @Override
            public void process(Long key, ValueState<TracingQueue> state) throws Exception {
                long startTime = System.currentTimeMillis();
                boolean contain = false;
                int i = 0;
                System.out.println(i);
                for(TracingPoint point:state.value().getQueueArray()) {
                    if(contains(window,point)) {
                        contain = true;
                        // 已经和一个范围相交,不需要继续判断
                        break;
                    }
                    i++;
                }
                long endTime = System.currentTimeMillis();
                out.collect(Tuple3.of(window,contain,endTime-startTime));
            }
        });
    }

    private boolean contains(Window window,TracingPoint point) {
        return point.getX() >= window.getXmin() &&
                point.getX() <= window.getXmax() &&
                point.getY() >= window.getYmin() &&
                point.getY() <= window.getYmax();
    }
}
