package com.wjf.trajectory.FlinkBase.operator;

import entity.TracingPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Dataloader implements FlatMapFunction<String, TracingPoint> {
    @Override
    public void flatMap(String line, Collector<TracingPoint> out) throws Exception {
        String[] strs = line.split(",");
        long id = Long.parseLong(strs[0]);
        double x = Double.parseDouble(strs[2]);
        if (x < 116.0) x = 116.0;
        else if (x > 116.8) x = 116.8;
        double y = Double.parseDouble(strs[3]);
        if (y < 39.5) y = 39.5;
        else if (y > 40.3) y = 40.3;
        out.collect(new TracingPoint(x, y, dateToTimestamp(strs[1], "yyyy-MM-dd HH:mm:ss"),id));
    }

    public long dateToTimestamp(String date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
}
