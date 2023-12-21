package util;

import entity.TracingPoint;

public class PointTool {
    /**
     * 默认地球半径
     */
    private static double EARTH_RADIUS = 6371000;//赤道半径(单位m)

    /**
     * 转化为弧度(rad)
     * */
    private static double rad(double d)
    {
        return d * Math.PI / 180.0;
    }
    /**
     * @param lon1 第一点的精度
     * @param lat1 第一点的纬度
     * @param lon2 第二点的精度
     * @param lat2 第二点的纬度
     * @return 返回的距离，单位m
     * */
    public static double getDistance(double lon1,double lat1,double lon2, double lat2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double sa2 = Math.sin((radLat1 - radLat2) / 2.0);
        double sb2 = Math.sin(((lon1 - lon2) * Math.PI / 180.0) / 2.0);
        return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1) * Math.cos(lat2) * sb2 * sb2));
    }

    public static double getDistance(TracingPoint p1,TracingPoint p2) {
        return getDistance(p1.x,p1.y,p2.x,p2.y);
    }
}
