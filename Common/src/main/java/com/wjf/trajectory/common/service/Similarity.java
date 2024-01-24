package com.wjf.trajectory.common.service;

import com.wjf.trajectory.common.entity.TracingQueue;

import java.io.Serializable;

public interface Similarity extends Serializable {
    double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory) ;
}
