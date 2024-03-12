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
    long step;
    private Segment segment = new TimeSegment();
    private double xMin = Double.MAX_VALUE;
    private double xMax = Double.MIN_VALUE;
    private double yMin = Double.MAX_VALUE;
    private double yMax = Double.MIN_VALUE;
//    private double historyXmin = Double.MAX_VALUE;
//    private double historyxMax = Double.MIN_VALUE;
//    private double historyyMin = Double.MAX_VALUE;
//    private double historyyMax = Double.MIN_VALUE;
    private int[] a;
    private int[] b;
    private int[] c;
    private int[] d;
    private int pointNum=0;
    private boolean out = false;
    int rate = 2;
    public int updateNum=0;
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
    public TracingQueue(long maxSize,long step) {
        this.maxQueueSize = maxSize;
        this.queueArray = new ArrayDeque<>();
        this.front = 0;
        this.rear = 0;
        this.step = step;
        a = new int[100];
        b = new int[100];
        c = new int[100];
        d = new int[100];
    }

    /**
     * 添加元素
     * @param point 待添加点
     * @return 元素位置
     */
    public int EnCircularQueue(TracingPoint point){
        // 添加上限设置
        while (queueArray.size() >= maxQueueSize) {
            for(int i = 0; i < step;i++) {
                queueArray.pollFirst();
            }
        }
//        double midX = xMin + (xMax-xMin)/2;
//        double midY = yMin+(yMax-yMin)/2;
//        int curA = point.x < midX&&point.y<midY?1:0;
//        int curB = point.x > midX&&point.y<midY?1:0;
//        int curC = point.x < midX&&point.y>midY?1:0;
//        int curD = point.x > midX&&point.y>midY?1:0;
//        pointNum++;
//
//        int xx=100;
//        int prePoint = pointNum+100-1;
//        int prePointA = a[prePoint%xx];
//        int prePointB = b[prePoint%xx];
//        int prePointC = c[prePoint%xx];
//        int prePointD = d[prePoint%xx];
//        a[pointNum%xx] = prePointA+curA;
//        b[pointNum%xx] = prePointB+curB;
//        c[pointNum%xx] = prePointC+curC;
//        d[pointNum%xx] = prePointD+curD;
//        int ret = 0;
//        if(pointNum>xx&&pointNum%rate==0){
//            int preA = a[(pointNum+rate)%xx];
//            int preB = b[(pointNum+rate)%xx];
//            int preC = c[(pointNum+rate)%xx];
//            int preD = d[(pointNum+rate)%xx];
//            int restA = a[pointNum%xx]-preA;
//            int restB = b[pointNum%xx]-preB;
//            int restC = c[pointNum%xx]-preC;
//            int restD = d[pointNum%xx]-preD;
//            if(restA==0&&restB==0&&restC==0&&restD!=0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//            else if(restA==0&&restB==0&&restC!=0&&restD==0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//            else if(restA==0&&restB!=0&&restC==0&&restD==0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//            else if(restA!=0&&restB==0&&restC==0&&restD==0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//            else if(restA==0&&restB==0&&restC!=0&&restD!=0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//            else if(restA==0&&restB!=0&&restC==0&&restD!=0){
//                updateNum++;
//                ret=1;
//                reset();
//            }
//        }
//        if(point.x > historyxMax||point.y>historyyMax||point.x<historyXmin||point.y<historyyMin){
//            this.out = true;
//        }
        updateMBR(point);
        queueArray.offerLast(point);
        return 0;
    }
    private void reset(){
        a = new int[100];
        b = new int[100];
        c = new int[100];
        d = new int[100];
        pointNum = 0;
    }

    /**
     * 添加元素
     * @param point 待添加点
     * @return 元素位置
     */
    public boolean EnCircularQueueWithOutStep(TracingPoint point){
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
