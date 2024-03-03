package com.wjf.trajectory.common.service.similarity;

import org.apache.commons.math3.util.Pair;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.ArrayDeque;
import java.util.Deque;

public class InClosestPairDistance implements Similarity {
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory,int step) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        Deque<Pair<Integer,Double>> stack = new ArrayDeque<>();
        double min = Integer.MAX_VALUE;
        for(int i = step; i > 0;i--) {
            TracingPoint source = firstTrace[firstTrace.length - i];
            for (TracingPoint point : secondTrace) {
                min = Math.min(PointTool.getDistance(point, source), min);
            }
            while (stack.size() > 0 && min < stack.peekLast().getSecond()) {
                stack.pollLast();
            }
            stack.offerLast(new Pair<>(firstTrace.length - i,min));
        }
        if(stack.peekFirst() ==null){
            return min;
        }
        return stack.peekFirst().getSecond();
    }
}
