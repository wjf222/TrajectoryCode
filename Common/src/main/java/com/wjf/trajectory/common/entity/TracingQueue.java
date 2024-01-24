package com.wjf.trajectory.common.entity;

import com.wjf.trajectory.common.indexs.z2.XZ2SFC;
import lombok.Data;
import com.wjf.trajectory.common.util.segment.Segment;
import com.wjf.trajectory.common.util.segment.TimeSegment;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * 存储轨迹点的循环队列
 */
@Data
public class TracingQueue implements Serializable {

    public Deque<TracingPoint> queueArray;
    public long index;
    public long id = -1;
    int front;
    int rear;
    long maxQueueSize;
    private Segment segment = new TimeSegment();
    private double xMin = Double.MAX_VALUE;
    private double xMax = Double.MIN_VALUE;
    private double yMin = Double.MAX_VALUE;
    private double yMax = Double.MIN_VALUE;
    public TracingQueue(){
        this.maxQueueSize = 10;
        this.queueArray = new ArrayDeque<>();
        this.front = 0;
        this.rear = 0;
    }
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
        updateMBR(point);
        return queueArray.offerLast(point);
    }

    private void updateMBR(TracingPoint point) {
        if(point.getX() < xMin) {
            xMin = point.getX();
        }
        if(point.getY() < yMin) {
            yMin = point.getY();
        }
        if(point.getX() > xMax) {
            xMax = point.getX();
        }
        if(point.getY() > yMax) {
            yMax = point.getY();
        }
    }
    /**
     * 添加元素
     * @param point 待添加点
     * @return 元素位置
     */
    public boolean EnCircularQueue(TracingPoint point,boolean isSegment){
        // 添加上限设置
        while (queueArray.size() >= maxQueueSize) {
            queueArray.pollFirst();
        }
        if(isSegment) {
            segmentProcessing();
        }
        return queueArray.offerLast(point);
    }
    public void updateId(long id) {
        this.id = id;
    }

    /**
     * 轨迹分段
     */
    private void segmentProcessing(){
        List<List<TracingPoint>> segments = segment.processing(queueArray);
        if(segments.size() == 0) {
            return;
        }
        // 只关心最新一段轨迹
        List<TracingPoint> segment = segments.get(segments.size() - 1);
        Deque<TracingPoint> newTrajectory = new ArrayDeque<>();
        for (TracingPoint point: segment) {
            newTrajectory.offerLast(point);
        }
        this.queueArray = newTrajectory;
    }
    /**
     * 基于单调栈和轨迹分段进行过滤
     */
    public void updateIndex(XZ2SFC xz2SFC) {
        index = xz2SFC.index(xMin, yMin, xMax, yMax,true);
    }
}
