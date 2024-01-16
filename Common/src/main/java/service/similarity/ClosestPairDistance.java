package service.similarity;

import entity.TracingQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

@Data
@AllArgsConstructor
public class ClosestPairDistance implements Similarity {
    // 可以结合单调栈实现增量计算
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        double ans = Integer.MAX_VALUE;
        for (TracingPoint point : first) {
            for (TracingPoint tracingPoint : second) {
                ans = Math.min(PointTool.getDistance(point, tracingPoint), ans);
            }
        }
        return ans;
    }
}
