package com.wjf.trajectory.common.partition;

import com.wjf.trajectory.common.entity.QueryPair;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class QueryPairKeySelector implements KeySelector<QueryPair, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> getKey(QueryPair pair) throws Exception {
        return Tuple2.of(pair.queryTra.id, pair.anotherTra.id);
    }
}
