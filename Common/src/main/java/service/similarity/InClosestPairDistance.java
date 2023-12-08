package service.similarity;

import org.apache.commons.math3.util.Pair;
import entity.TracingPoint;
import entity.TracingQueue;
import service.Similarity;
import util.PointTool;

import java.util.Deque;

public class InClosestPairDistance implements Similarity {
    private final TracingQueue tracingQueue;
    private final Deque<Pair<Integer,Double>> stack;

    public InClosestPairDistance(TracingQueue tracingQueue, Deque<Pair<Integer, Double>> stack) {
        this.tracingQueue = tracingQueue;
        this.stack = stack;
    }

    @Override
    public double compute(TracingPoint[] first, TracingPoint[] target) {
        TracingPoint source = first[first.length-1];
        double min = Integer.MAX_VALUE;
        for (TracingPoint point : target) {
            min = Math.min(PointTool.getDistance(point, source), min);
        }
        while (stack.size() > 0 && min < stack.peekLast().getSecond()) {
            stack.pollLast();
        }
        int rear = tracingQueue.EnCircularQueue(source);
        if(stack.size() >0 && stack.peekFirst().getFirst() == rear){
            stack.pollFirst();
        }
        stack.offerLast(new Pair<>(rear,min));
        return stack.peekFirst().getSecond();
    }
}
