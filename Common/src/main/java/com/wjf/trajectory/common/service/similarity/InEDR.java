package com.wjf.trajectory.common.service.similarity;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;

public class InEDR implements Similarity {
    private double[] lastResult;

    public InEDR(int length) {
        lastResult = new double[length+1];
    }
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source =firstTrace[firstTrace.length-1];
        double[] result = new double[lastResult.length];
        for(int j = 1; j <=secondTrace.length;j++) {
            int subcost = PointTool.getDistance(source,secondTrace[j-1]) <= 50 ? 0 : 1;
            result[j] = Math.min(getLastResult(lastResult,j-1) + subcost, Math.min(getLastResult(lastResult,j) + 1, result[j - 1] + 1));
        }
        lastResult = result;
        return result[secondTrace.length];
    }

    private double getLastResult(double[] lastResult, int i) {
        if(i >= lastResult.length){
            return 0;
        }
        return lastResult[i];
    }
}
