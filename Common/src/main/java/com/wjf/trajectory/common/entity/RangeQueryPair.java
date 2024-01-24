package com.wjf.trajectory.common.entity;

import lombok.Data;
@Data
public class RangeQueryPair {
    long tracingQueueId;
    public boolean contain = false;
    public long startTimestamp = Long.MAX_VALUE;
    public long endTimestamp = 0;
}
