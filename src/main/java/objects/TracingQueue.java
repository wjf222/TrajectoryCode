package objects;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 存储轨迹点的循环队列
 */
public class TracingQueue {
    TracingPoint[] queueArray;
    int front;
    int rear;
    int maxQueueSize;
    public TracingQueue(int maxSize) {
        this.maxQueueSize = maxSize;
        this.queueArray = new TracingPoint[maxSize];
        this.front = 0;
        this.rear = 0;
    }
    public int EnCircularQueue(TracingPoint point){
        if ((rear + 1) % maxQueueSize == front){
            return -1;
        }
        queueArray[rear] = point;
        int ans = rear;
        //循环队列在rear指针时，如果只是简单累加，很可能会出现空指针异常
        rear = (rear + 1) % maxQueueSize;
        return ans;
    }

    public int DeCircularQueue (){
        //满足front = rear时，说明队列为空
        if (front == rear){return -1;}
        Integer ans = front;
        front = (front + 1) % maxQueueSize;
        return ans;
    }
    public boolean isFull(){
        return (rear + 1) % maxQueueSize == front;
    }
}
