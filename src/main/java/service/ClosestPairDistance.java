package service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import objects.TracingPoint;
import objects.TracingQueue;
import org.apache.commons.math3.util.Pair;
import util.PointTool;

import java.util.ArrayDeque;
import java.util.Deque;

@Data
@AllArgsConstructor
public class ClosestPairDistance implements Similarity{
    private final TracingQueue tracingQueue;
    private final TracingPoint[] first;
    private Deque<Pair<Integer,Double>> stack;
    public ClosestPairDistance(){
        this.first = null;
        this.tracingQueue = null;
    }
    public ClosestPairDistance(TracingPoint[] first){
        this.tracingQueue = new TracingQueue(10);
        this.first = first;
        this.stack = new ArrayDeque<>();
    }
    // 可以结合单调栈实现增量计算
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second) {
        int la = first.length;
        int lb = second.length;
        double ans = Integer.MAX_VALUE;
        for(int i = 0; i < la;i++){
            for(int j = 0; j < lb;j++){
                ans = Math.min(PointTool.getDistance(first[i],second[j]),ans);
            }
        }
        return ans;
    }

    public double incrementCompute(TracingPoint p){
        double min = Integer.MAX_VALUE;
        for (TracingPoint point : first) {
            min = Math.min(PointTool.getDistance(point, p), min);
        }
        while (stack.size() > 0 && min < stack.peekLast().getSecond()) {
            stack.pollLast();
        }
        int rear = tracingQueue.EnCircularQueue(p);
        if(stack.size() >0 && stack.peekFirst().getFirst() == rear){
            stack.pollFirst();
        }
        stack.offerLast(new Pair<>(rear,min));
        return stack.peekFirst().getSecond();
    }
}
