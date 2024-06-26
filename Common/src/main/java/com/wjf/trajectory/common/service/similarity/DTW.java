package com.wjf.trajectory.common.service.similarity;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;


public class DTW implements Similarity {
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step){
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        int firstSize = first.size();
        int secondSize = second.size();
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        double[][] D0 = new double[firstSize+1][secondSize+1];
        for(int i = 0; i < firstSize;i++){
            for(int j = 0; j < secondSize;j++){
                D0[i][j] = Math.abs(PointTool.getDistance(firstTrace[i].x,firstTrace[i].y,secondTrace[j].x,secondTrace[j].y));
            }
        }
        return calculation(D0);
    }

    private double calculation(double[][] x){
        int r = x.length-1;
        int c = x[0].length-1;
        double[][] D = new double[r][c];
        for (int i = 0; i < r; i++) {
            System.arraycopy(x[i + 1], 1, D[i], 0, c);
        }

        //计算损失矩阵M
        double[][] MC = x.clone();
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                if(i == 0 && j != 0) {
                    MC[i][j] += MC[i][j-1];
                } else if(i != 0 && j == 0){
                    MC[i][j] += MC[i-1][j];
                } else if(i != 0){
                    MC[i][j] += Math.min(Math.min(MC[i - 1][j - 1], MC[i][j - 1]), MC[i - 1][j]);
                }
            }
        }
        return MC[r - 1][c - 1];
    }
}
