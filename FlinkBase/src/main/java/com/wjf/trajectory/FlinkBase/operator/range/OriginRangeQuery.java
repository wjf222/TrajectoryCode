package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import entity.TracingPoint;
import indexs.commons.Window;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OriginRangeQuery extends KeyedProcessFunction<Long, RangeQueryPair,RangeQueryPair> {
    @Override
    public void processElement(RangeQueryPair pair, KeyedProcessFunction<Long, RangeQueryPair, RangeQueryPair>.Context ctx, Collector<RangeQueryPair> out) throws Exception {
        boolean contain = false;
        for(TracingPoint point:pair.getTracingQueue().getQueueArray()) {
            if(contains(pair.getWindow(),point)) {
                pair.setContain(true);
                // 已经和一个范围相交,不需要继续判断
                break;
            }
        }
        out.collect(pair);
    }

    private boolean contains(Window window, TracingPoint point) {
        return point.getX() >= window.getXmin() &&
                point.getX() <= window.getXmax() &&
                point.getY() >= window.getYmin() &&
                point.getY() <= window.getYmax();
    }
}
