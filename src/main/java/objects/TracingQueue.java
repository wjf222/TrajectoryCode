package objects;

/**
 * 存储轨迹点的循环队列
 */
public class TracingQueue {
    TracingPoint[] queueArray = null;
    int front;
    int rear;
    int maxQueueSize;
    public TracingQueue(int maxSize) {
        this.maxQueueSize = maxSize;
        this.queueArray = new TracingPoint[maxSize];
        this.front = 0;
        this.rear = 0;
    }
    public void EnCircularQueue(TracingPoint point){
        if ((rear + 1) % maxQueueSize == front){
            return;
        }
        queueArray[rear] = point;
        //循环队列在rear指针时，如果只是简单累加，很可能会出现空指针异常
        rear = (rear + 1) % maxQueueSize;
    }

    public TracingPoint DeCircularQueue (){
        //满足front = rear时，说明队列为空
        if (front == rear){return null;}
        TracingPoint returnElem = queueArray[front];
        queueArray[front] = null;
        front = (front + 1) % maxQueueSize;
        return returnElem;
    }
}
