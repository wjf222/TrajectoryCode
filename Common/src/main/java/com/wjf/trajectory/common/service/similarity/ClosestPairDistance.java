package com.wjf.trajectory.common.service.similarity;

import com.wjf.trajectory.common.entity.TracingQueue;
import lombok.AllArgsConstructor;
import lombok.Data;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;

@Data
@AllArgsConstructor
public class ClosestPairDistance implements Similarity {
    // 可以结合单调栈实现增量计算
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step) {
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
