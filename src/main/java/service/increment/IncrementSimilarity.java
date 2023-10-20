package service.increment;

import pojo.TracingPoint;

// 增量轨迹相似度计算
public interface IncrementSimilarity {
    double compute(TracingPoint source, TracingPoint[] target);
}
