package entity;

import lombok.Data;

import java.io.Serializable;

/**
 * 存储轨迹点的循环队列
 */
@Data
public class TracingQueue implements Serializable {
    public TracingPoint[] queueArray;
    public long id = -1;
    int front;
    int rear;
    long maxQueueSize;
    public TracingQueue(){}
    public TracingQueue(long maxSize) {
        this.maxQueueSize = maxSize;
        this.queueArray = new TracingPoint[(int)maxSize];
        this.front = 0;
        this.rear = 0;
    }

    /**
     * 添加元素
     * @param point 待添加点
     * @return 元素位置
     */
    public int EnCircularQueue(TracingPoint point){
        if ((rear + 1) % maxQueueSize == front){
            return -1;
        }
        updateId(point.id);
        queueArray[rear] = point;
        int ans = rear;
        //循环队列在rear指针时，如果只是简单累加，很可能会出现空指针异常
        rear = (int) ((rear + 1) % maxQueueSize);
        return ans;
    }

    /**
     * 删除元素
     * @return 元素位置
     */
    public int DeCircularQueue (){
        //满足front = rear时，说明队列为空
        if (front == rear){return -1;}
        Integer ans = front;
        front = (int) ((front + 1) % maxQueueSize);
        return ans;
    }
    public boolean isEmpty() {
        return front == rear;
    }
    public boolean isFull(){
        return (rear + 1) % maxQueueSize == front;
    }

    public void updateId(long id) {
        if(this.isEmpty()) {
            this.id = id;
        }
    }
}
