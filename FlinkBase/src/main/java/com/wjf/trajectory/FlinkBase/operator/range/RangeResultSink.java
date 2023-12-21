package com.wjf.trajectory.FlinkBase.operator.range;

import com.google.gson.Gson;
import entity.QueryPair;
import entity.RangeQueryResult;
import indexs.commons.Window;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class RangeResultSink extends RichSinkFunction<Tuple2<Window, List<Long>>> {
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
    public void invoke(Tuple2<Window, List<Long>> result, Context context) throws Exception {
        super.invoke(result, context);
        // 没有数据则不需要聚合
        if(result.f1.size() == 0) {
            return ;
        }
        Window window = result.f0;
        String fileName = String.format("%f-%f-%f-%f.txt", window.getXmin(), window.getYmin(), window.getXmax(), window.getYmax());
        //写入文件
        String filePath = sinkDir + fileName;
        File file = new File(filePath);
        if (!file.exists()) file.createNewFile();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file,true))) {
            for (Long value : result.f1) {
                writer.write(value.toString());
                writer.newLine();
            }
            writer.write("---"); // 用分隔符隔开不同的列表
            writer.newLine();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        File sinkDirFile = new File(sinkDir);
        List<String> result = new ArrayList<>();
        for (File sinkFile : sinkDirFile.listFiles()) {
            if (sinkFile.getName().contains("0_aggregate.txt")) continue;
            List<Long> mergedList = readListsFromFile(sinkFile.getPath());
            if(mergedList.size() != 0) {
                // 结果去重
                String record = mergedList.stream()
                        .distinct()
                        .sorted()
                        .map(Objects::toString)
                        .collect(Collectors.joining(", "));
                RangeQueryResult rangeQueryResult = new RangeQueryResult(sinkFile.getName(), record, mergedList.size());
                // 创建 Gson 对象
                Gson gson = new Gson();
                String jsonString = gson.toJson(rangeQueryResult);
                result.add(jsonString);
            }
        }
        //聚合搜索结果
        String aggFileName = String.format("%s0_aggregate.txt",sinkDir);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(aggFileName))) {
            for (String value : result) {
                writer.write(value);
                writer.newLine();
            }
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
                mergedList.add(Long.parseLong(line));
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
