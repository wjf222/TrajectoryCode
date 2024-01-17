package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import indexs.commons.Window;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class RangeResultSink extends RichSinkFunction<RangeQueryPair> {
    public String sinkDir;

    public RangeResultSink(String sinkDir) {
        this.sinkDir = sinkDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        File sinkDirFile = new File(sinkDir);
        deleteDirRecursively(sinkDirFile);
        if (!sinkDirFile.exists()) sinkDirFile.mkdirs();
    }

    @Override
    public void invoke(RangeQueryPair result, Context context) throws Exception {
        super.invoke(result, context);
        Window window = result.getWindow();
        String fileName = String.format("%f-%f-%f-%f.txt", window.getXmin(), window.getYmin(), window.getXmax(), window.getYmax());
        //写入文件
        String filePath = sinkDir + fileName;
        File file = new File(filePath);
        if (!file.exists()) file.createNewFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file,true))) {
            writer.write(String.format("TrajectoryId=%d,contain=%s,start=%d,end=%d,time(ms)=%d"
                    ,result.getTracingQueue().id
                    ,result.isContain()
                    ,result.startTimestamp
                    ,result.endTimestamp
                    ,result.endTimestamp-result.startTimestamp));
            writer.newLine();
            writer.write("---"); // 用分隔符隔开不同的列表
            writer.newLine();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        File sinkDirFile = new File(sinkDir);
        List<String> result = new ArrayList<>();
        long count = 0;
        long total_time = 0;
        long avg_time = 0;
        for (File sinkFile : sinkDirFile.listFiles()) {
            if (sinkFile.getName().contains("0_aggregate.txt")) continue;
            List<Long> mergedList = readListsFromFile(sinkFile.getPath());
            if(mergedList.size() != 0) {
                // 结果去重
                for(long time:mergedList){
                    count++;
                    total_time = total_time+time;
                }
            }
        }
        //聚合搜索结果
        String aggFileName = String.format("%s0_aggregate.txt",sinkDir);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(aggFileName))) {
            writer.write(String.format("total_time=%d,total_count=%d,avg_time=%d",total_time,count,total_time/count));
            writer.newLine();
        }
    }

    public static List<Long> readListsFromFile(String filename) {
        List<Long> mergedList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("---")) {
                    continue; // 跳过分隔符行
                }
                String[] split = line.split(",");
                String cost = split[4].split("=")[1 ];
                mergedList.add(Long.parseLong(cost));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mergedList;
    }
    public static void deleteDirRecursively(File file) {
        if (!file.exists()) return;
        if (file.isDirectory()) {
            File[] childrenFile = file.listFiles();
            for (File childFile : childrenFile) {
                deleteDirRecursively(childFile);
            }
        } else {
            file.delete();
        }
    }
}
