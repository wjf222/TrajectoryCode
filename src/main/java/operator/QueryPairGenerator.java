package operator;

import entity.QueryPair;
import entity.QueryTraInfo;
import entity.TracingPoint;
import entity.TracingQueue;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class QueryPairGenerator extends KeyedCoProcessFunction<Long, QueryTraInfo, TracingPoint, QueryPair> {

    public long timeWindow;

    //缓存query来之前的tra
    private ValueState<TracingQueue> traState;

    public QueryPairGenerator(long timeWindow) {
        this.timeWindow = timeWindow;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext().getState(
                new ValueStateDescriptor<TracingQueue>("traState", TracingQueue.class, new TracingQueue(timeWindow))
        );
    }

    @Override
    public void processElement1(QueryTraInfo info, KeyedCoProcessFunction<Long, QueryTraInfo, TracingPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.value();
        if (tra.id == -1) return; //尚未形成
        tra = SerializationUtils.clone(tra);

        if (info.queryTra.id == tra.id) {
            out.collect(new QueryPair(info.queryTra, info.queryTra, info.info.threshold));
            return;
        }
    }

    @Override
    public void processElement2(TracingPoint point, KeyedCoProcessFunction<Long, QueryTraInfo, TracingPoint, QueryPair>.Context ctx, Collector<QueryPair> out) throws Exception {
        TracingQueue tra = traState.value();
        tra.EnCircularQueue(point);
        traState.update(tra);
    }
}
