package com.wjf.trajectory.common.indexs.commons;

import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.WindowPoint;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 由于范围查询需要提供访问的四个端点
 * 因此提供本类抽象四个端点
 * @author 王纪锋
 */
@Data
@AllArgsConstructor
public class Window {
    private double xmin;
    private double ymin;
    private double xmax;
    private double ymax;


    public Window(double xmin,double ymin,double xmax,double ymax){
        this.xmin = xmin;
        this.ymin = ymin;
        this.xmax = xmax;
        this.ymax = ymax;
        pointList = new ArrayList<>();
    }
    //多边形的点
    private List<WindowPoint> pointList;
    @Override
    public int hashCode(){
        int code = 17;
        long mDoubleXMin = Double.doubleToLongBits(xmin);
        long mDoubleYMin = Double.doubleToLongBits(ymin);
        long mDoubleXMax = Double.doubleToLongBits(xmax);
        long mDoubleYMax = Double.doubleToLongBits(ymax);
        code = 31*code + (int)(mDoubleXMin ^ (mDoubleXMin >>> 32));
        code = 31*code + (int)(mDoubleYMin ^ (mDoubleYMin >>> 32));
        code = 31*code + (int)(mDoubleXMax ^ (mDoubleXMax >>> 32));
        code = 31*code + (int)(mDoubleYMax ^ (mDoubleYMax >>> 32));
        return code;
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(obj == null){
            return false;
        }
        if(!(obj instanceof TracingPoint)){
            return false;
        }
        TracingPoint a = (TracingPoint) obj;
        return Math.abs(xmin-a.getX()) < 0.00001
                && Math.abs(ymin-a.getY()) < 0.00001
                && Math.abs(xmax-a.getY()) < 0.00001
                && Math.abs(ymax-a.getY()) < 0.00001;
    }

    public String toString() {
        return String.format("{xMin:%f,yMin:%f,xMax:%f,yMax:%f}",xmin,ymin,xmax,ymax);
    }
}