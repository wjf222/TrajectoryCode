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
        // 添加上限设置
        while (queueArray.size() >= maxQueueSize) {
            queueArray.pollFirst();
        }
        return queueArray.offerLast(point);
    }

    public void updateId(long id) {
        this.id = id;
    }
}
