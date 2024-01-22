package com.wjf.trajectory.FlinkBase.operator.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PartitionRangeResultSink extends RichSinkFunction<Tuple2<Integer,Long>> {
    public String sinkDir;

    public PartitionRangeResultSink(String sinkDir) {
        this.sinkDir = sinkDir;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().getJobId();
        File sinkDirFile = new File(sinkDir);
        deleteDirRecursively(sinkDirFile);
        if (!sinkDirFile.exists()) sinkDirFile.mkdirs();
    }

    @Override
    public void invoke(Tuple2<Integer,Long> result, Context context) throws Exception {
        super.invoke(result, context);
        Integer subTaskIndex = result.f0;
        long endTime = result.f1;
        String fileName = String.format("%s.txt",subTaskIndex);
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

        //聚合搜索结果
        String aggFileName = String.format("%s0_aggregate.txt",sinkDir);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(aggFileName))) {
            List<Tuple3<Long,Long,Long>> list = new ArrayList<>();
            for (File sinkFile : sinkDirFile.listFiles()) {
                long count = 0;
                long total_time = 0;
                if (sinkFile.getName().contains("0_aggregate.txt")) continue;
                List<Long> mergedList = readListsFromFile(sinkFile.getPath());
                if(mergedList.size() != 0) {
                    for(long time:mergedList){
                        count++;
                        total_time = total_time+time;
                    }
                }
                long avg_time = total_time/count;
                list.add(Tuple3.of(total_time,count,avg_time));
            }
            Collections.sort(list, new Comparator<Tuple3<Long, Long, Long>>() {
                @Override
                public int compare(Tuple3<Long, Long, Long> subTask1, Tuple3<Long, Long, Long> subTask2) {
                    Long total_time1 = subTask1.f0;
                    Long total_time2 = subTask2.f0;
                    return (int)(total_time1-total_time2);
                }
            });
            long total_time_avg = 0;
            long total_count_avg = 0;
            List<Long> timeList = new ArrayList<>();
            List<Long> countList = new ArrayList<>();
            for(int i = 0; i < list.size();i++) {
                timeList.add(list.get(i).f0/1000);
                countList.add(list.get(i).f1);
            }
            total_time_avg = getMean(timeList);
            total_count_avg = getMean(countList);
            writer.write(String.format("subTaskSum:%d,total_time_avg(ms)=%d,total_count_avg=%d", list.size(), total_time_avg,total_count_avg));
            writer.newLine();
            final long sd_time = getStandardDeviation(timeList);
            final long sd_count = getStandardDeviation(countList);
            writer.write(String.format("subTaskSum:%d,sd_time(ms)=%d,sd_count=%d", list.size(), sd_time,sd_count));
            writer.newLine();

            // 每一个slot与标准值的偏差
            final long mean_time = total_time_avg;
            final long mean_count = total_count_avg;
            List<Double> sdMeanTimeDouble = timeList.stream().map(time -> Math.abs((double)(time-mean_time)/(double) sd_time)).collect(Collectors.toList());
            List<Double> sdMeanCountDouble = countList.stream().map(count ->Math.abs ((double)(count-mean_count)/(double) sd_count)).collect(Collectors.toList());

            double sumSDTime = sdMeanTimeDouble.stream().mapToDouble(time -> time).sum();
            double sumSDCount = sdMeanCountDouble.stream().mapToDouble(time -> time).sum();
            writer.write(String.format("subTaskSum:%d,sumSDDiffTime(ms)=%.3f,sumSDDiffCount=%.3f", list.size(), sumSDTime,sumSDCount));
            writer.newLine();
            List<String> timeStringList = timeList.stream().map(String::valueOf).collect(Collectors.toList());
            List<String> countStringList = countList.stream().map(String::valueOf).collect(Collectors.toList());
            writer.write(String.format("total_time:%s",String.join(",",timeStringList)));
            writer.newLine();
            writer.write(String.format("total_count:%s",String.join(",",countStringList)));
            writer.newLine();
            List<String> sdMeanTimeString = sdMeanTimeDouble.stream().map(String::valueOf).collect(Collectors.toList());
            List<String> sdMeanCountString = sdMeanCountDouble.stream().map(String::valueOf).collect(Collectors.toList());
            writer.write(String.format("sdMeanTime:%s",String.join(",",sdMeanTimeString)));
            writer.newLine();
            writer.write(String.format("sdMeanCount:%s",String.join(",",sdMeanCountString)));
            writer.newLine();
            for(int i = 0; i < list.size();i++) {
                writer.write(String.format("subTaskIndex:%s,total_time(ms)=%d,total_count=%d,avg_time(us)=%d", i, list.get(i).f0 / 1000, list.get(i).f1, list.get(i).f2));
                writer.newLine();
            }
        }
    }
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
