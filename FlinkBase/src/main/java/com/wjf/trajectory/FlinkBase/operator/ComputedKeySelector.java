package com.wjf.trajectory.FlinkBase.operator;

import com.wjf.trajectory.common.entity.QueryPair;
import org.apache.flink.api.java.functions.KeySelector;

public class ComputedKeySelector implements KeySelector<QueryPair, Long> {
    @Override
    public Long getKey(QueryPair pair) throws Exception {
        return null;
    }
}
