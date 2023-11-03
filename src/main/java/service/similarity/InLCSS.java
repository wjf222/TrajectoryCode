package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

public class InLCSS implements Similarity {
    private double[] lastResult;
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] target) {
        TracingPoint source = first[first.length-1];
        double[] result = new double[lastResult.length+1];
        for(int j = 0; j < target.length;j++){
            if(PointTool.getDistance(source,target[j]) < 50){
                result[j + 1] = lastResult[j] + 1;
            } else {
                result[j + 1] = Math.max(lastResult[j+1],result[j]);
            }
        }
        lastResult = result;
        return result[target.length];
    }
}
