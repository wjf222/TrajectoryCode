package service.increment;

import entity.TracingPoint;
import util.PointTool;

import java.util.HashMap;
import java.util.Map;

public class ERP implements IncrementSimilarity {
    private double[] lastResult;
    private TracingPoint gap;

    @Override
    public double compute(TracingPoint source, TracingPoint[] target) {
        Map<TracingPoint, Double> mapGapDist = new HashMap<>();
        mapGapDist.put(source, PointTool.getDistance(source, gap));
        for (TracingPoint point : target) {
            mapGapDist.put(point, PointTool.getDistance(point, gap));
        }
        double[] result = new double[lastResult.length+1];
        for(int j = 1; j <= target.length;j++) {
            TracingPoint pointB = target[j - 1];
            result[j] = Math.min(lastResult[j-1]+PointTool.getDistance(source, pointB),
                    Math.min(lastResult[j]+mapGapDist.get(source),result[j-1]+mapGapDist.get(pointB)));
        }
        lastResult = result;
        return result[target.length];
    }
}
