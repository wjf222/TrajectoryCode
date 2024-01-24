package com.wjf.trajectory.common.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class QueryInfo implements Serializable {
    public long queryTraId;
    public double threshold;

    public QueryInfo() {
        queryTraId = -1;
    }

    public QueryInfo(long queryId, double threshold) {
        this.queryTraId = queryId;
        this.threshold = threshold;
    }
}
