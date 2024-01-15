package entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
public class TracingPoint implements Serializable {
    // 轨迹编号
    public long id;
    // 经度
    public double x;
    // 纬度
    public double y;
    // 时间
    public long date;

    // 用于分区
    public long shardKey;
    public TracingPoint(){}
    public TracingPoint(double x, double y, long date, long id) {
        this.x = x;
        this.y = y;
        this.date = date;
        this.id = id;
    }
    @Override
    public int hashCode(){
        int code = 17;
        long mDoubleLongitude = Double.doubleToLongBits(x);
        long mDoubleLatitude = Double.doubleToLongBits(y);
        code = 31*code + (int)(mDoubleLongitude ^ (mDoubleLongitude >>> 32));
        code = 31*code + (int)(mDoubleLatitude ^ (mDoubleLatitude >>> 32));
        code = 31*code + (int)(date ^ (date >>> 32));
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
        return id == a.id && Math.abs(x -a.getX()) < 0.00001
                && Math.abs(y -a.getY()) < 0.00001 && date == a.date;
    }
}
