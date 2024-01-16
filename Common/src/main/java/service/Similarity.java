package service;

import entity.TracingPoint;
import entity.TracingQueue;

import java.io.Serializable;
import java.util.Deque;

public interface Similarity extends Serializable {
    double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory) ;
}
