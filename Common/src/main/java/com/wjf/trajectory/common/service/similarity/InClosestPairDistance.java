package com.wjf.trajectory.common.service.similarity;

import org.apache.commons.math3.util.Pair;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import com.wjf.trajectory.common.service.Similarity;
import com.wjf.trajectory.common.util.PointTool;

import java.util.Deque;

public class InClosestPairDistance implements Similarity {
    private final TracingQueue tracingQueue;
    private final Deque<Pair<Integer,Double>> stack;

    public InClosestPairDistance(TracingQueue tracingQueue, Deque<Pair<Integer, Double>> stack) {
        this.tracingQueue = tracingQueue;
        this.stack = stack;
    }

    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory) {
        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint[] firstTrace = first.toArray(new TracingPoint[0]);
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        TracingPoint source = firstTrace[firstTrace.length-1];
        double min = Integer.MAX_VALUE;
        for (TracingPoint point : secondTrace) {
            min = Math.min(PointTool.getDistance(point, source), min);
        }
        while (stack.size() > 0 && min < stack.peekLast().getSecond()) {
            stack.pollLast();
        }
        // TODO 重新写一遍
//        int rear = tracingQueue.EnCircularQueue(source);
//        if(stack.size() >0 && stack.peekFirst().getFirst() == rear){
//            stack.pollFirst();
//        }
//        stack.offerLast(new Pair<>(rear,min));
        return stack.peekFirst().getSecond();
    }
}
