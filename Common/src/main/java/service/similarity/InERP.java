package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class InERP implements Similarity {
    private double[] lastResult;
    private TracingPoint gap;

    @Override
    public double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) {
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source = firstTrace[firstTrace.length-1];
        Map<TracingPoint, Double> mapGapDist = new HashMap<>();
        mapGapDist.put(source, PointTool.getDistance(source, gap));
        for (TracingPoint point : secondTrace) {
            mapGapDist.put(point, PointTool.getDistance(point, gap));
        }
        double[] result = new double[lastResult.length+1];
        for(int j = 1; j <= secondTrace.length;j++) {
            TracingPoint pointB = secondTrace[j - 1];
            result[j] = Math.min(lastResult[j-1]+PointTool.getDistance(source, pointB),
                    Math.min(lastResult[j]+mapGapDist.get(source),result[j-1]+mapGapDist.get(pointB)));
        }
        lastResult = result;
        return result[secondTrace.length];
    }
}
