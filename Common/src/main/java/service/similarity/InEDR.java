package service.similarity;

import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

public class InEDR implements Similarity {
    private double[] lastResult;

    public InEDR(int length) {
        lastResult = new double[length+1];
    }
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] target) {
        TracingPoint source =first[first.length-1];
        double[] result = new double[lastResult.length];
        for(int j = 1; j <=target.length;j++) {
            int subcost = PointTool.getDistance(source,target[j-1]) <= 50 ? 0 : 1;
            result[j] = Math.min(lastResult[j - 1] + subcost, Math.min(lastResult[j] + 1, result[j - 1] + 1));
        }
        lastResult = result;
        return result[target.length];
    }
}
