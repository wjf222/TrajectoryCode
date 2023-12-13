package service;

import entity.TracingPoint;

import java.io.Serializable;
import java.util.Deque;

public interface Similarity extends Serializable {
    double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) ;
}
