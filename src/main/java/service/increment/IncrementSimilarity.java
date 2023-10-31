package service.increment;

import entity.TracingPoint;

// 增量轨迹相似度计算
public interface IncrementSimilarity {
    double compute(TracingPoint source, TracingPoint[] target);
}
