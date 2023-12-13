package entity;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 存储轨迹点的循环队列
 */
@Data
public class TracingQueue implements Serializable {

    public Deque<TracingPoint> queueArray;
    public long id = -1;
    int front;
    int rear;
    long maxQueueSize;

    public TracingQueue(){}
    public TracingQueue(long maxSize) {
        this.maxQueueSize = maxSize;
        this.queueArray = new ArrayDeque<>();
        this.front = 0;
        this.rear = 0;
    }

    /**
     * 添加元素
     * @param point 待添加点
     * @return 元素位置
     */
    public boolean EnCircularQueue(TracingPoint point){
        return queueArray.offerLast(point);
    }

    /**
     * 删除元素
     * @return 元素位置
     */
    public TracingPoint DeCircularQueue (){
        //满足front = rear时，说明队列为空
        return queueArray.pollFirst();
    }
    public boolean isEmpty() {
        return front == rear;
    }
    public boolean isFull(){
        return (rear + 1) % maxQueueSize == front;
    }

    public void updateId(long id) {
        this.id = id;
    }
}
