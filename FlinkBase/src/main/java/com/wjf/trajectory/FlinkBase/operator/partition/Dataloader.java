package com.wjf.trajectory.FlinkBase.operator.partition;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wjf.trajectory.common.entity.TracingPoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.wjf.trajectory.common.util.math.Tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class Dataloader extends KeyedProcessFunction<Long,String,TracingPoint> {

    private int count = 1;
    private int numberOfParallelSubtasks;
    private Map<Long,List<Long>> slotTrajectory;
    private Map<Long,Long> trajectoryShardKey;
    public Dataloader(){
        trajectoryShardKey = new HashMap<>();
        slotTrajectory = new HashMap<>();
    }


    @Override
    public void processElement(String line, KeyedProcessFunction<Long, String, TracingPoint>.Context ctx, Collector<TracingPoint> out) throws Exception {
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
        if(count%500000 == 0) {
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, String, TracingPoint>.OnTimerContext ctx, Collector<TracingPoint> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        String[] metrics = {"PartitionRangeQuery.CustomPartition.CalTimeCounter","PartitionRangeQuery.CustomPartition.PointsCounter"};
        List<Tuple2<Integer,Long>> timeList = new ArrayList<>();
        List<Tuple2<Integer,Long>> countList = new ArrayList<>();

        for (int slotId = 0; slotId < numberOfParallelSubtasks; slotId++) {
            String timeCounter = getMetricSlot(host, port, vertexId, slotId, metrics[0]);
            String pointsCounter = getMetricSlot(host, port, vertexId, slotId, metrics[1]);
            if(timeCounter == null || pointsCounter==null){
                ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                return;
            }
            timeList.add(Tuple2.of(slotId,Long.parseLong(timeCounter)/1000));
            countList.add(Tuple2.of(slotId,Long.parseLong(pointsCounter)));
        }
        final long total_time_avg = Tools.getMean(timeList.stream().map(integerLongTuple2 -> integerLongTuple2.f1).collect(Collectors.toList()));
        final long total_count_avg = Tools.getMean(countList.stream().map(integerLongTuple2 -> integerLongTuple2.f1).collect(Collectors.toList()));
        final long sd_time = Tools.getStandardDeviation(timeList.stream().map(integerLongTuple2 -> integerLongTuple2.f1).collect(Collectors.toList()));
        final long sd_count = Tools.getStandardDeviation(countList.stream().map(integerLongTuple2 -> integerLongTuple2.f1).collect(Collectors.toList()));
        List<Tuple2<Integer,Double>> sdMeanTimeDouble = timeList.stream().map(tuple2 -> Tuple2.of(tuple2.f0,Math.abs((double)(tuple2.f1-total_time_avg)/(double) sd_time))).collect(Collectors.toList());
        List<Tuple2<Integer,Double>> sdMeanCountDouble = countList.stream().map(tuple2 ->Tuple2.of(tuple2.f0,Math.abs((double)(tuple2.f1-total_count_avg)/(double) sd_count))).collect(Collectors.toList());
        sdMeanTimeDouble.sort(Comparator.comparing(o -> o.f1));
        sdMeanCountDouble.sort(Comparator.comparing(o -> o.f1));
        if(sdMeanCountDouble.get(sdMeanTimeDouble.size()-1).f1>1){
            int maxSlot = sdMeanCountDouble.get(sdMeanTimeDouble.size()-1).f0;
            int minSlot = sdMeanCountDouble.get(0).f0;
            batchRandomSwap(slotTrajectory.get(maxSlot),slotTrajectory.get(minSlot),100);
        }
    }

    private String host;
    private String port;
    private String vertexId;
    public Dataloader(String host, String port) throws IOException {
        this.host = host;
        this.port = port;
        trajectoryShardKey = new HashMap<>();
        slotTrajectory = new HashMap<>();
    }

    private void batchRandomSwap(List<Long> list1,List<Long> list2,int batchSize) {
        Random random = new Random();
        long sharkKey1 =trajectoryShardKey.get(list1.get(0));
        long sharkKey2 =trajectoryShardKey.get(list2.get(0));
        for(int i = 0; i < batchSize;i++){
            int index = random.nextInt(Math.min(list1.size(),list2.size()));
            trajectoryShardKey.put(list1.get(index),sharkKey2);
            trajectoryShardKey.put(list2.get(index),sharkKey1);
            swap(list1,list2,index);
        }
    }
    private void swap(List<Long> list1,List<Long> list2,int index){
        long tmp = list1.get(index);
        list1.set(index,list2.get(index));
        list2.set(index,tmp);
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.vertexId = getMetric(this.host,this.port);
        numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    }


    public long generateShardKey(long id){
        if(!trajectoryShardKey.containsKey(id)){
            trajectoryShardKey.put(id,id%8);
        }
        return trajectoryShardKey.get(id);
    }
    public long dateToTimestamp(String date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(date, formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }
    // URL url = new URL("http://<jobmanager_host>:<port>/jobs/<job_id>/vertices/<vertex_id>/metrics");
    public String getMetric(String host,String port) throws IOException {
        String jobID = getRuntimeContext().getJobId().toString();
        URL jobInfoUrl = new URL(String.format("http://%s:%s/jobs/%s/",host,port,jobID));
        String response = request(jobInfoUrl);

        JsonObject jsonObject = JsonParser.parseString(response).getAsJsonObject();
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


    /**
     * 指标示例: http://172.27.65.247:8090/jobs/b516d940c40b2a97436449b2cb0f1f14/vertices/65315653e6a72108fdcdd0c29a17976f/subtasks/0/metrics?get=checkpointAlignmentTime
     * http://<host>:<port>/jobs/<jobId>/vertices/<vertexId>/subtasks/<subTaskId>/metrics?get=<metricName>
     * @param host
     * @param port
     * @param vertexId
     * @param slotId
     * @param metricName
     * @return
     */
    private static String getMetricSlot(String host,String port,String vertexId,int slotId,String metricName) throws IOException {
        URL metricUrl = new URL(String.format("http://%s:%s/jobs/%s/verticces/%s/subtasks/%d/metrics?get=%s"
                ,host,port,vertexId,slotId,metricName));
        String response = request(metricUrl);
        JsonArray asJsonArray = JsonParser.parseString(response).getAsJsonArray();
        if(asJsonArray.size() == 0) {
            return null;
        }
        return asJsonArray.get(0).getAsJsonObject().get("value").toString();
    }

    private static String request(URL url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

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
        return response.toString();
    }
}
