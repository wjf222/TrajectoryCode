package indexs.commons;

import entity.TracingPoint;
import lombok.AllArgsConstructor;
import lombok.Data;

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
        return Math.abs(xmin-a.getLongitude()) < 0.00001
                && Math.abs(ymin-a.getLatitude()) < 0.00001
                && Math.abs(xmax-a.getLatitude()) < 0.00001
                && Math.abs(ymax-a.getLatitude()) < 0.00001;
    }
}