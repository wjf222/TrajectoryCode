package service.similarity;

import pojo.TracingPoint;

public interface Similarity {
    double compute(TracingPoint[] first, TracingPoint[] second) ;
}
