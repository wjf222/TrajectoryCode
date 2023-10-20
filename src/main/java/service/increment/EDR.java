package service.increment;

import pojo.TracingPoint;
import util.PointTool;

public class EDR implements IncrementSimilarity{
    private double[] lastResult;

    public EDR(int length) {
        lastResult = new double[length+1];
    }
    @Override
    public double compute(TracingPoint source, TracingPoint[] target) {
        double[] result = new double[lastResult.length];
        for(int j = 1; j <=target.length;j++) {
            int subcost = PointTool.getDistance(source,target[j-1]) <= 50 ? 0 : 1;
            result[j] = Math.min(lastResult[j - 1] + subcost, Math.min(lastResult[j] + 1, result[j - 1] + 1));
        }
        lastResult = result;
        return result[target.length];
    }
}
