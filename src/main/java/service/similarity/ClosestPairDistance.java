package service.similarity;

import lombok.AllArgsConstructor;
import lombok.Data;
import entity.TracingPoint;
import util.PointTool;

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
