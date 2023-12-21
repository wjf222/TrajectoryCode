package com.wjf.trajectory.FlinkBase.operator.job;

import entity.QueryPair;
import entity.TracingQueue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import service.Similarity;

public class SimilarCalculator extends KeyedProcessFunction<Tuple2<Long, Long>, QueryPair, QueryPair> {
    public Similarity incrementSimilarity;

    public SimilarCalculator(Similarity incrementSimilarity) {
        this.incrementSimilarity = incrementSimilarity;
    }
    @Override
    public void processElement(QueryPair pair, KeyedProcessFunction<Tuple2<Long, Long>, QueryPair, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue queryTra = pair.queryTra;
        TracingQueue anotherTra = pair.anotherTra;
        boolean identity = queryTra.id == anotherTra.id;
        pair.similarityDistance = identity?0.0:incrementSimilarity.compute(queryTra,anotherTra);
        if (!identity && pair.similarityDistance <= pair.threshold) pair.numSimilarTra++;
        out.collect(pair);
    }
}
