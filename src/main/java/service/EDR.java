package service;

import objects.TracingPoint;
import util.PointTool;

public class EDR implements Similarity{
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second) {
        int m = first.length;
        int n = second.length;
        int[][] dp = new int[m+1][n+1];
        for(int i = 0; i <= m;i++) dp[i][0] = i;
        for(int i = 0; i <= n;i++) dp[0][i] = i;

        for(int i = 1; i <= m;i++) {
            for(int j = 1; j <=n;j++) {
                int subcost = PointTool.getDistance(first[i-1],second[j-1]) <= 50 ? 0 : 1;
                dp[i][j] = Math.min(dp[i - 1][j - 1] + subcost, Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
            }
        }
        return dp[m][n];
    }
}
