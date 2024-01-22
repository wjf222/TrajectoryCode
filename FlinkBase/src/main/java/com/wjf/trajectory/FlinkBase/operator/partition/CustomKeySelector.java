package com.wjf.trajectory.FlinkBase.operator.partition;

import entity.TracingPoint;
import org.apache.flink.api.java.functions.KeySelector;

public class CustomKeySelector implements KeySelector<TracingPoint, Long> {

    @Override
    public Long getKey(TracingPoint point) throws Exception {
        return point.getId();
    }
}
