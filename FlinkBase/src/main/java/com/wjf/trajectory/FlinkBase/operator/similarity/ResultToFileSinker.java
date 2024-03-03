package com.wjf.trajectory.FlinkBase.operator.similarity;

import com.wjf.trajectory.common.entity.QueryPair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ResultToFileSinker extends RichSinkFunction<QueryPair> {
    public String sinkDir;

    public ResultToFileSinker(String sinkDir) {
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
    public void invoke(QueryPair queryPair, Context context) throws Exception {
        super.invoke(queryPair, context);
//        System.out.printf("SimiResult： %s\n",queryPair.toString());

        //写入文件
        String filePath = sinkDir + queryPair.getQueryTraId() + ".txt";
        File file = new File(filePath);
        if (!file.exists()) file.createNewFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file,true))) {
            writer.write(queryPair.toString());
            writer.newLine();
            writer.write("---"); // 用分隔符隔开不同的列表
            writer.newLine();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //聚合搜索结果
        File aggFile = new File(sinkDir + "0_aggregate.txt");
        aggFile.createNewFile();
        FileWriter aggWriter = new FileWriter(aggFile);

        int cnt = 0;
        int validCnt = 0;
        long startTimestamp = Long.MAX_VALUE;
        long endTimestamp = 0;
        long totalTime = 0;

        File sinkDirFile = new File(sinkDir);
        for (File sinkFile : sinkDirFile.listFiles()) {

            if (sinkFile.getName().contains("0_aggregate.txt")) continue;
            cnt++;
            List<Long> mergedList = readListsFromFile(sinkFile.getPath());
            if(mergedList.size() != 0) {
                // 结果去重
                for(long time:mergedList){
                    totalTime = totalTime+time;
                }
            }
        }
        long avgTime = 0;
        if (cnt > 0) avgTime = totalTime/cnt;
        aggWriter.write(String.format(
                "[AGGREGATE]total=%d(valid=%d),total_time(ms)=%d(start=%d,end=%d),avg_time(ms)=%d\n",
                cnt, validCnt, totalTime, startTimestamp, endTimestamp, avgTime
        ));
        aggWriter.flush();
        aggWriter.close();

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
    public static List<Long> readListsFromFile(String filename) {
        List<Long> mergedList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("---")) {
                    continue; // 跳过分隔符行
                }
                String[] strs = line.split(",");
                if(strs.length < 5){
                    continue;
                }
                Long cost = Long.parseLong( strs[4].split("=")[1]);
                mergedList.add(cost);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mergedList;
    }
}
