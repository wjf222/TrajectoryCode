package com.wjf.trajectory.FlinkBase.operator.range;

import indexs.commons.Window;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class RangeInfoLoader extends KeyedProcessFunction<Integer,String, Window> {

    /**
     * 将字符串数组转换为查询窗口
     * @param value 按”xmin,ymin,xmax,ymax“保存
     */
    @Override
    public void processElement(String value, KeyedProcessFunction<Integer, String, Window>.Context ctx, Collector<Window> out) throws Exception {
        double[] points = Arrays.stream(value.split(","))
                .mapToDouble(Double::parseDouble)
                .toArray();
        Window window = new Window(points[0],points[1],points[2],points[3]);
        out.collect(window);
    }
}
