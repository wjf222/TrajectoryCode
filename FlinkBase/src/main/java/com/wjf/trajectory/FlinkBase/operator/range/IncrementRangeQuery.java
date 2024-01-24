package com.wjf.trajectory.FlinkBase.operator.range;

import com.wjf.trajectory.common.entity.RangeQueryPair;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.indexs.commons.Window;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class IncrementRangeQuery extends KeyedProcessFunction<Long, RangeQueryPair,RangeQueryPair> {
    ValueState<Boolean> containValueState;
    ValueStateDescriptor<Boolean> containValueStateDescriptor;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        containValueStateDescriptor= new ValueStateDescriptor<>(
                "contain",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                false
        );
        containValueState = getRuntimeContext().getState(containValueStateDescriptor);
    }

    @Override
    public void processElement(RangeQueryPair pair, KeyedProcessFunction<Long, RangeQueryPair, RangeQueryPair>.Context ctx, Collector<RangeQueryPair> out) throws Exception {
//        if(containValueState.value() || contains(pair.getWindow(),pair.getTracingQueue().getQueueArray().getLast())){
//            pair.setContain(true);
//            containValueState.update(true);
//        }
        out.collect(pair);
    }

    private boolean contains(Window window, TracingPoint point) {
        return point.getX() >= window.getXmin() &&
                point.getX() <= window.getXmax() &&
                point.getY() >= window.getYmin() &&
                point.getY() <= window.getYmax();
    }
}
