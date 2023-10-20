package service.increment;

import org.apache.commons.math3.util.Pair;
import pojo.TracingPoint;
import pojo.TracingQueue;
import util.PointTool;

import java.util.Deque;

public class ClosestPairDistance implements IncrementSimilarity{
    private final TracingQueue tracingQueue;
    private final Deque<Pair<Integer,Double>> stack;

    public ClosestPairDistance(TracingQueue tracingQueue, Deque<Pair<Integer, Double>> stack) {
        this.tracingQueue = tracingQueue;
        this.stack = stack;
    }

    @Override
    public double compute(TracingPoint source, TracingPoint[] target) {
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
