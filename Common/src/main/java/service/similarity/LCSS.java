package service.similarity;

import lombok.extern.slf4j.Slf4j;
import entity.TracingPoint;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

@Slf4j
public class LCSS implements Similarity {
    public double threshold;
    public int delta;
    public LCSS(double threshold, int delta) {
        this.threshold = threshold;
        this.delta = delta;
    }
    @Override
    public double compute(Deque<TracingPoint> first, Deque<TracingPoint> second) {
        int la = first.size();
        int lb = second.size();
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        int[][] dp = new int[la+1][lb+1];
        for(int i = 0; i < la;i++){
            for(int j = 0; j < lb;j++){
                if(PointTool.getDistance(firstTrace[i],secondTrace[j]) < 50){
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    dp[i + 1][j + 1] = Math.max(dp[i][j+1],dp[i+1][j]);
                }
            }
        }
        return dp[la][lb];
    }
}
