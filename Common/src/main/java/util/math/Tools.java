package util.math;

import java.util.List;

public class Tools {
    public static long getStandardDeviation(List<Long> data) {
        long mean = getMean(data);
        long sum = 0L;
        for (long num : data) {
            sum += Math.pow(num - mean, 2);
        }
        long sd = (long) Math.sqrt(sum / (data.size() - 1));
        return sd;
    }

    public static long getMean(List<Long> data) {
        long sum = 0L;
        for (long num : data) {
            sum += num;
        }
        long mean = sum / data.size();
        return mean;
    }

    public static Long currentMicrosecond(){
        return System.nanoTime()/1000;
    }
}
