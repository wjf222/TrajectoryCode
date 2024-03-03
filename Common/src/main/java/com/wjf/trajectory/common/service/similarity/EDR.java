package com.wjf.trajectory.common.service.similarity;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;

public class EDR implements Similarity {

    public double threshold;

    public EDR(double threshold){
        this.threshold = threshold;
    }
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        int m = firstTrace.length;
        int n = secondTrace.length;
        int[][] dp = new int[m+1][n+1];
        for(int i = 0; i <= m;i++) dp[i][0] = i;
        for(int i = 0; i <= n;i++) dp[0][i] = i;

        for(int i = 1; i <= m;i++) {
            for(int j = 1; j <=n;j++) {
                int subcost = PointTool.getDistance(firstTrace[i-1],secondTrace[j-1]) <= 50 ? 0 : 1;
                dp[i][j] = Math.min(dp[i - 1][j - 1] + subcost, Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
            }
        }
        return dp[m][n];
    }
}
