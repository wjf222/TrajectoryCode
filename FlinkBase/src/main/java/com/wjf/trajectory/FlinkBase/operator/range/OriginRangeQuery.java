package com.wjf.trajectory.FlinkBase.operator.range;

import entity.RangeQueryPair;
import entity.TracingPoint;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class OriginRangeQuery extends KeyedProcessFunction<Long, RangeQueryPair,RangeQueryPair> {
    @Override
    public void processElement(RangeQueryPair pair, KeyedProcessFunction<Long, RangeQueryPair, RangeQueryPair>.Context ctx, Collector<RangeQueryPair> out) throws Exception {
//        for(TracingPoint point:pair.getTracingQueue().getQueueArray()) {
//            if(contains(pair.getWindow(),point)) {
//                pair.setContain(true);
//            }
//        }
        out.collect(pair);
    }

    public static boolean isPointInsidePolygon(TracingPoint point, List<TracingPoint> polygon) {
        int intersectCount = 0;
        for (int i = 0; i < polygon.size(); i++) {
            TracingPoint p1 = polygon.get(i);
            TracingPoint p2 = polygon.get((i + 1) % polygon.size());

            // 检查水平射线是否与边相交
            if (((p1.y > point.y) != (p2.y > point.y)) &&
                    (point.x < (p2.x - p1.x) * (point.y - p1.y) / (p2.y - p1.y) + p1.x)) {
                intersectCount++;
            }
        }

        // 奇数次相交意味着点在多边形内
        return (intersectCount % 2 == 1);
    }
}
