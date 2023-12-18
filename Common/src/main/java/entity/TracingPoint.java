package entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class TracingPoint implements Serializable {
    // 轨迹编号
    public long id;
    // 经度
    public double longitude;
    // 纬度
    public double latitude;
    // 时间
    public long date;

    public TracingPoint(){}
    public TracingPoint(double longitude, double latitude, long date, long id) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.date = date;
        this.id = id;
    }
    @Override
    public int hashCode(){
        int code = 17;
        long mDoubleLongitude = Double.doubleToLongBits(longitude);
        long mDoubleLatitude = Double.doubleToLongBits(latitude);
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
        return id == a.id && Math.abs(longitude-a.getLongitude()) < 0.00001
                && Math.abs(latitude-a.getLatitude()) < 0.00001 && date == a.date;
    }
}
