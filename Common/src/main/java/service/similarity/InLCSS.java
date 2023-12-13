package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

public class InLCSS implements Similarity {
    private double[] lastResult;
    @Override
    public double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) {
        TracingPoint source = first.peekLast();
        int secondLength = second.size();
        double[] result = new double[secondLength+1];
        if(lastResult == null) {
            lastResult = new double[secondLength+1];
        }
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        for(int j = 0; j < secondLength;j++){
            if(PointTool.getDistance(source,secondTrace[j]) < 50){
                result[j + 1] = lastResult[j] + 1;
            } else {
                result[j + 1] = Math.max(lastResult[j+1],result[j]);
            }
        }
        lastResult = result;
        return result[secondLength];
    }
}
