package com.wjf.trajectory.FlinkBase.operator.util;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BroadWindowSource extends RichSourceFunction<String> {
    private final String filePath;
    private volatile boolean isRunning = true;
    public BroadWindowSource(String filePath) {
        this.filePath = filePath;
    }
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null && isRunning) {
                ctx.collect(line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file: " + filePath, e);
        }
        ctx.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
