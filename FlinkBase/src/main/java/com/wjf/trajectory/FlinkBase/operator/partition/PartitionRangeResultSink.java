package com.wjf.trajectory.FlinkBase.operator.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class PartitionRangeResultSink extends RichSinkFunction<Tuple2<String,Long>> {
    public String sinkDir;

    public PartitionRangeResultSink(String sinkDir) {
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
    public void invoke(Tuple2<String,Long> result, Context context) throws Exception {
        super.invoke(result, context);
        String subTask = result.f0;
        long endTime = result.f1;
        String fileName = String.format("%s.txt",subTask);
        //写入文件
        String filePath = sinkDir + fileName;
        File file = new File(filePath);
        if (!file.exists()) file.createNewFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file,true))) {
            writer.write(String.format("time(us)=%d",endTime));
            writer.newLine();
            writer.write("---"); // 用分隔符隔开不同的列表
            writer.newLine();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        File sinkDirFile = new File(sinkDir);
        long count = 0;
        long total_time = 0;
        //聚合搜索结果
        String aggFileName = String.format("%s0_aggregate.txt",sinkDir);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(aggFileName))) {
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
                long avg_time = total_time/count;
                writer.write(String.format("subTask:%s,total_time(ms)=%d,total_count=%d,avg_time(us)=%d",sinkFile.getName(),total_time/1000,count,avg_time));
                writer.newLine();
            }
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
                String cost = line.split("=")[1 ];
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
