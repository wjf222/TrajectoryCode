package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import entity.TracingPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Dataloader extends RichFlatMapFunction<String, TracingPoint> {

    public Dataloader(){

    }
    private String host;
    private String port;
    private String vertexId;
    public Dataloader(String host,String port) throws IOException {
        this.host = host;
        this.port = port;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void flatMap(String line, Collector<TracingPoint> out) throws Exception {
        this.vertexId = getMetric(this.host,this.port);
        System.out.printf("VertexId:%s",vertexId);
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
    // URL url = new URL("http://<jobmanager_host>:<port>/jobs/<job_id>/vertices/<vertex_id>/metrics");
    public String getMetric(String host,String port) throws IOException {
        String jobID = getRuntimeContext().getJobId().toString();
        URL jobInfoUrl = new URL(String.format("http://%s:%s/jobs/%s/",host,port,jobID));
        String.format("http://%s:%s/jobs/%s/vertices/<vertex_id>/metrics",host,port,jobID);

        HttpURLConnection con = (HttpURLConnection) jobInfoUrl.openConnection();

        con.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        // 解析返回的 JSON 数据以获取指标
        con.disconnect();

        JsonObject jsonObject = JsonParser.parseString(response.toString()).getAsJsonObject();
        String vertexId = null;
        if (jsonObject.has("vertices")) {
            JsonArray vertices = jsonObject.getAsJsonArray("vertices");
            for (int i = 0; i < vertices.size(); i++) {
                JsonObject vertex = vertices.get(i).getAsJsonObject();
                if(vertex.get("name").getAsString().contains("PartitionRangeQuery")){
                    vertexId = vertex.get("id").getAsString();
                }
            }
        }
        return vertexId;
    }
}
