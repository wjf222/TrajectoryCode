package com.wjf.trajectory.common.service.similarity;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class InERP implements Similarity {
    private double[] lastResult;
    private TracingPoint gap;

    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source = firstTrace[firstTrace.length-1];
        Map<TracingPoint, Double> mapGapDist = new HashMap<>();
        mapGapDist.put(source, PointTool.getDistance(source, gap));
        for (TracingPoint point : secondTrace) {
            mapGapDist.put(point, PointTool.getDistance(point, gap));
        }
        double[] result = new double[lastResult.length+1];
        for(int j = 1; j <= secondTrace.length;j++) {
            TracingPoint pointB = secondTrace[j - 1];
            result[j] = Math.min(getLastResult(lastResult,j-1)+PointTool.getDistance(source, pointB),
                    Math.min(getLastResult(lastResult,j)+mapGapDist.get(source),result[j-1]+mapGapDist.get(pointB)));
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
