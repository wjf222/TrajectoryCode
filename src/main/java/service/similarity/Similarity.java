package service.similarity;

import entity.TracingPoint;

import java.io.Serializable;

public interface Similarity extends Serializable {
    double compute(TracingPoint[] first, TracingPoint[] second) ;
}
