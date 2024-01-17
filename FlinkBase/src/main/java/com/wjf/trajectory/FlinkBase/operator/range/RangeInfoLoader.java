package com.wjf.trajectory.FlinkBase.operator.range;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Rectangle;
import entity.TracingPoint;
import indexs.commons.Window;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RangeInfoLoader extends KeyedProcessFunction<Integer,String, Window> {

    private ListState<Window> windowListState;
    private ListStateDescriptor<Window> windowListStateDescriptor;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        windowListStateDescriptor = new ListStateDescriptor<Window>(
                "windowListState",
                TypeInformation.of(new TypeHint<Window>() {
                }));
        windowListState = getRuntimeContext().getListState(windowListStateDescriptor);
    }

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
//        windowListState.add(window);
//        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime()+10*1000L);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, Window>.OnTimerContext ctx, Collector<Window> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<Window> windows = new ArrayList<>();
        windowListState.get().forEach(window -> windows.add(window));
        out.collect(windows.get(0));
        windows.remove(0);
        windowListState.update(windows);
    }
}
