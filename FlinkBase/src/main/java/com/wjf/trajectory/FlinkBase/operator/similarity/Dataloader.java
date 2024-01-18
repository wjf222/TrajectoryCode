package com.wjf.trajectory.FlinkBase.operator.similarity;

import entity.TracingPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Dataloader extends RichFlatMapFunction<String, TracingPoint> {
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

        TracingPoint point = TracingPoint.builder()
                .id(id)
                .date(dateToTimestamp(strs[1], "yyyy-MM-dd HH:mm:ss"))
                .x(x)
                .y(y)
                .shardKey(generateShardKey(id))
                .build();
        out.collect(point);
    }

    public long generateShardKey(long id){
        return id % 8;
    }
    public long dateToTimestamp(String date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }

//    public int getMetric(){
//
//        String jobId = String.valueOf(getRuntimeContext().getJobId());
//        getRunt
//        String jobManagerAddress = "http://localhost:8081"; // Flink JobManager 地址和端口
//
//        // 构建 REST API 请求 URL
//        String restUrl = jobManagerAddress + "/jobs/" + jobId + "/metrics";
//
//        // 发送 HTTP GET 请求获取指标信息
//        HttpURLConnection conn = (HttpURLConnection) new URL(restUrl).openConnection();
//        conn.setRequestMethod("GET");
//
//    }
}
