package service;

import objects.TracingPoint;

public interface Similarity {
    double compute(TracingPoint[] first, TracingPoint[] second) ;
}
