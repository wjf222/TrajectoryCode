package com.wjf.trajectory.common.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class QueryPair implements Serializable {
    public TracingQueue queryTra;
    public TracingQueue anotherTra;
    private long queryTraId;
    public double threshold;

    public double similarityDistance = Double.MAX_VALUE;
    public long startTimestamp = Long.MAX_VALUE;
    public long endTimestamp = 0;

    //for reduce
    public boolean reduced = false;
    public int numSimilarTra = 0;
    public int numTotalTra = 1;

    public QueryPair(){}
    public QueryPair(TracingQueue queryTra,TracingQueue anotherTra,double threshold) {
        this.queryTra = queryTra;
        this.anotherTra = anotherTra;
        this.threshold = threshold;
    }

    //for reduce
    public QueryPair(QueryPair value1, QueryPair value2) {
        this.reduced = true;
        this.queryTra = value1.queryTra;
        this.startTimestamp = Math.min(value1.startTimestamp, value2.startTimestamp);
        this.endTimestamp = Math.max(value1.endTimestamp, value2.endTimestamp);
        this.numTotalTra = value1.numTotalTra + value2.numTotalTra;
        this.numSimilarTra = value1.numSimilarTra + value2.numSimilarTra;
    }

    @Override
    public String toString() {
        long time = endTimestamp - startTimestamp;
        return String.format("queryId=%d,similar=%d(total=%d),start=%d,end=%d,time(ms)=%d\n", queryTraId, numSimilarTra, numTotalTra, startTimestamp, endTimestamp, time);
    }
}
