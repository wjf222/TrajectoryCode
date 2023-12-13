package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

public class InEDR implements Similarity {
    private double[] lastResult;

    public InEDR(int length) {
        lastResult = new double[length+1];
    }
    @Override
    public double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) {
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source =firstTrace[firstTrace.length-1];
        double[] result = new double[lastResult.length];
        for(int j = 1; j <=secondTrace.length;j++) {
            int subcost = PointTool.getDistance(source,secondTrace[j-1]) <= 50 ? 0 : 1;
            result[j] = Math.min(lastResult[j - 1] + subcost, Math.min(lastResult[j] + 1, result[j - 1] + 1));
        }
        lastResult = result;
        return result[secondTrace.length];
    }
}
