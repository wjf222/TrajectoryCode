package com.wjf.trajectory.FlinkBase.operator.util;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TextSourceFunction extends RichSourceFunction<String> {
    private final String filePath;
    private volatile boolean isRunning = true;
    public TextSourceFunction(String filePath) {
        this.filePath = filePath;
    }
    @Override
    public void run(SourceContext ctx) throws Exception {
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = reader.readLine()) != null && isRunning) {
                    ctx.collect(line);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading from file: " + filePath, e);
            }

            // 在文件读取完毕后，循环会继续，从而重新打开文件进行读取
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
