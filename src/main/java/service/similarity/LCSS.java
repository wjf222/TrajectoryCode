package service.similarity;

import lombok.extern.slf4j.Slf4j;
import entity.TracingPoint;
import util.PointTool;

import java.io.Serializable;

@Slf4j
public class LCSS implements Similarity {
    public double threshold;
    public int delta;
    public LCSS(double threshold, int delta) {
        this.threshold = threshold;
        this.delta = delta;
    }
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second) {
        int la = first.length;
        int lb = second.length;
        int[][] dp = new int[la+1][lb+1];
        for(int i = 0; i < la;i++){
            for(int j = 0; j < lb;j++){
                if(PointTool.getDistance(first[i],second[j]) < 50){
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    dp[i + 1][j + 1] = Math.max(dp[i][j+1],dp[i+1][j]);
                }
            }
        }
        return dp[la][lb];
    }
}
