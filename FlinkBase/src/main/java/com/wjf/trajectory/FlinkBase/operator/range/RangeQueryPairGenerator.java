package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import entity.TracingPoint;
import indexs.commons.Window;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

public class RangeQueryPairGenerator extends BroadcastProcessFunction<TracingPoint, Window, RangeQueryPair> {
    private ValueState<Set<Window>> windowRangeSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Set<Window>> valueStateDescriptor = new ValueStateDescriptor<>(
                "WindowRangeSet",
                TypeInformation.of(new TypeHint<Set<Window>>() {
                })
        );
        windowRangeSet = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(TracingPoint value, BroadcastProcessFunction<TracingPoint, Window, RangeQueryPair>.ReadOnlyContext ctx, Collector<RangeQueryPair> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(Window value, BroadcastProcessFunction<TracingPoint, Window, RangeQueryPair>.Context ctx, Collector<RangeQueryPair> out) throws Exception {
        Set<Window> set = windowRangeSet.value();
        set.add(value);
        windowRangeSet.update(set);
    }
}
