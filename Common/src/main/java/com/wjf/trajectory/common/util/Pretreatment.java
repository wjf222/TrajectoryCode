package com.wjf.trajectory.common.util;

import com.wjf.trajectory.common.entity.TracingPoint;

public class Pretreatment {
    /**
     * 经度最小值
     */
    private static final double MIN_LONGTITUDE = -180.0;

    /**
     * 经度最大值
     */
    private static final double MAX_LONGTITUDE = 180.0;

    /**
     * 纬度最小值
     */
    private static final double MIN_LATITUDE = -90.0;

    /**
     * 纬度最大值
     */
    private static final double MAX_LATITUDE = 90.0;

    public static boolean positionRange(TracingPoint point) {
        double lon = point.getX();
        double lat = point.getY();
        return positionRange(lon,lat);
    }
    public static boolean positionRange(double lon,double lat) {
        if (lon <= MIN_LONGTITUDE || lon >= MAX_LONGTITUDE) {
            throw new IllegalArgumentException(String.format("经度取值范围为(%f, %f),输入为(%f)", MIN_LONGTITUDE, MAX_LONGTITUDE,lon));
        }
        if (lat <= MIN_LATITUDE || lat >= MAX_LATITUDE) {
            throw new IllegalArgumentException(String.format("纬度取值范围为(%f, %f)", MIN_LATITUDE, MAX_LATITUDE));
        }
        return true;
    }
}
