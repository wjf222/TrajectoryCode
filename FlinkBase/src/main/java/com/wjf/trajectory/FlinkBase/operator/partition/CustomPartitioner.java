package com.wjf.trajectory.FlinkBase.operator.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartitioner<T> implements Partitioner<T> {

    @Override
    public int partition(T key, int numPartitions) {
        // Simple partitioning logic for 2 keys and 2 partitions
        return (Integer) key%numPartitions;
    }
}