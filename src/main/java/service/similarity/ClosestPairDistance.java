package service.similarity;

import lombok.AllArgsConstructor;
import lombok.Data;
import pojo.TracingPoint;
import pojo.TracingQueue;
import org.apache.commons.math3.util.Pair;
import util.PointTool;

import java.util.ArrayDeque;
import java.util.Deque;

@Data
@AllArgsConstructor
public class ClosestPairDistance implements Similarity {
    // 可以结合单调栈实现增量计算
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second) {
        int la = first.length;
        int lb = second.length;
        double ans = Integer.MAX_VALUE;
        for (TracingPoint point : first) {
            for (TracingPoint tracingPoint : second) {
                ans = Math.min(PointTool.getDistance(point, tracingPoint), ans);
            }
        }
        return ans;
    }
}
