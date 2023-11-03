package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

public class InDTW implements Similarity {
    private double[] lastResult;
    private boolean init;

    public InDTW(int length) {
        lastResult = new double[length];
        init = false;
    }
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] target) {
        TracingPoint source = first[first.length-1];
        double[] dist = new double[target.length];
        for(int j = 0; j < target.length;j++){
            dist[j] = Math.abs(PointTool.getDistance(source.longitude,source.latitude,target[j].longitude,target[j].latitude));
        }
        for (int j = 0; j < target.length; j++) {
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
        return lastResult[target.length-1];
    }
}
