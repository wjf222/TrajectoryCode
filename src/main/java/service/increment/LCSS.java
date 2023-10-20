package service.increment;

import pojo.TracingPoint;
import util.PointTool;

public class LCSS implements IncrementSimilarity{
    private double[] lastResult;
    @Override
    public double compute(TracingPoint source, TracingPoint[] target) {
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
