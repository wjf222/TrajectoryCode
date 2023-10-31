package service.similarity;

import entity.TracingPoint;
import util.PointTool;

import java.util.HashMap;
import java.util.Map;

public class ERP implements Similarity {

    private TracingPoint gap;

    public ERP(TracingPoint gap){
        this.gap = gap;
    }
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second) {
        Map<TracingPoint,Double> mapGapDist = new HashMap<>();
        for(TracingPoint point:first) {
            mapGapDist.put(point, PointTool.getDistance(point,gap));
        }
        for(TracingPoint point:second) {
            mapGapDist.put(point,PointTool.getDistance(point,gap));
        }
        int m = first.length;
        int n = second.length;
        double[][] dp = new double[m+1][n+1];
        dp[0][0] = 0;
        double sum = 0;
        for(int i = 1; i <= m;i++) {
            sum += mapGapDist.get(first[i-1]);
            dp[i][0] = sum;
        }
        sum = 0;
        for(int i = 1; i <= n;i++) {
            sum += mapGapDist.get(second[i-1]);
            dp[0][i] = sum;
        }
        for(int i = 1; i <= m; i++) {
            TracingPoint pointA = first[i - 1];
            for(int j = 1; j <= n;j++) {
                TracingPoint pointB = second[j - 1];
                dp[i][j] = Math.min(dp[i-1][j-1]+PointTool.getDistance(pointA, pointB),
                        Math.min(dp[i-1][j]+mapGapDist.get(pointA),dp[i][j-1]+mapGapDist.get(pointB)));
            }
        }
        return dp[m][n];
    }
}
