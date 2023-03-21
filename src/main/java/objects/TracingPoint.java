package objects;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
@Builder
public class TracingPoint {
    // 轨迹编号
    public int id;
    // 经度
    public double longitude;
    // 纬度
    public double latitude;
    // 时间
    public long date;


}
