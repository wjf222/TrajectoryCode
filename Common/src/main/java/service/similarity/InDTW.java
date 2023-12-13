package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

public class InDTW implements Similarity {
    private double[] lastResult;
    private boolean init;

    public InDTW(int length) {
        lastResult = new double[length];
        init = false;
    }
    @Override
    public double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) {
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source = firstTrace[firstTrace.length-1];
        double[] dist = new double[secondTrace.length];
        for(int j = 0; j < secondTrace.length;j++){
            dist[j] = Math.abs(PointTool.getDistance(source.longitude,source.latitude,secondTrace[j].longitude,secondTrace[j].latitude));
        }
        for (int j = 0; j < secondTrace.length; j++) {
            if(!init && j != 0) {
                dist[j] += dist[j-1];
            } else if(init && j == 0){
                dist[j] += lastResult[j];
            } else if(init){
                dist[j] += Math.min(Math.min(lastResult[j - 1], dist[j - 1]), lastResult[j]);
            }
        }
        lastResult = dist;
        if(!init) init = true;
        return lastResult[secondTrace.length-1];
    }
}
