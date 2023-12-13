package service.similarity;

import entity.TracingPoint;
import entity.TracingQueue;
import service.Similarity;
import util.PointTool;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class ERP implements Similarity {

    private TracingPoint gap;

    public ERP(TracingPoint gap){
        this.gap = gap;
    }
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue secondTrajectory) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = secondTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        Map<TracingPoint,Double> mapGapDist = new HashMap<>();
        for(TracingPoint point:first) {
            mapGapDist.put(point, PointTool.getDistance(point,gap));
        }
        for(TracingPoint point:second) {
            mapGapDist.put(point,PointTool.getDistance(point,gap));
        }
        int m = firstTrace.length;
        int n = secondTrace.length;
        double[][] dp = new double[m+1][n+1];
        dp[0][0] = 0;
        double sum = 0;
        for(int i = 1; i <= m;i++) {
            sum += mapGapDist.get(firstTrace[i-1]);
            dp[i][0] = sum;
        }
        sum = 0;
        for(int i = 1; i <= n;i++) {
            sum += mapGapDist.get(secondTrace[i-1]);
            dp[0][i] = sum;
        }
        for(int i = 1; i <= m; i++) {
            TracingPoint pointA = firstTrace[i - 1];
            for(int j = 1; j <= n;j++) {
                TracingPoint pointB = secondTrace[j - 1];
                dp[i][j] = Math.min(dp[i-1][j-1]+PointTool.getDistance(pointA, pointB),
                        Math.min(dp[i-1][j]+mapGapDist.get(pointA),dp[i][j-1]+mapGapDist.get(pointB)));
            }
        }
        return dp[m][n];
    }
}
