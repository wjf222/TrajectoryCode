package com.wjf.trajectory.FlinkBase.operator;

import entity.QueryTraInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoBroadcaster implements FlatMapFunction<QueryTraInfo,QueryTraInfo> {
    public int dataSize;

    public QueryTraInfoBroadcaster(int datasize){
        this.dataSize = datasize;
    }
    @Override
    public void flatMap(QueryTraInfo queryTraInfo, Collector<QueryTraInfo> collector) throws Exception {
        for (long anotherId = 1; anotherId <= dataSize; anotherId++) {
            collector.collect(new QueryTraInfo(queryTraInfo, anotherId));
        }
    }
}
