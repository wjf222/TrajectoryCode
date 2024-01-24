package operator;

import com.wjf.trajectory.common.entity.QueryInfo;
import com.wjf.trajectory.common.entity.QueryTraInfo;
import com.wjf.trajectory.common.entity.TracingPoint;
import com.wjf.trajectory.common.entity.TracingQueue;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoGenerator extends KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo> {

    public long timeWindow;
    // 轨迹状态
    private ValueState<TracingQueue> traState;
    private ValueState<QueryInfo> queryInfoValueState;
    private ValueState<Integer> queryNum;
    public  QueryTraInfoGenerator(long timeWindow) {
        this.timeWindow = timeWindow;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext()
                .getState(new ValueStateDescriptor<TracingQueue>("traState",TracingQueue.class,new TracingQueue(timeWindow)));
        queryInfoValueState = getRuntimeContext()
                .getState(new ValueStateDescriptor<QueryInfo>("queryInfoValueState",QueryInfo.class,new QueryInfo()));
    }

    @Override
    public void processElement1(TracingPoint value, KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        TracingQueue tra = traState.value();
        tra.EnCircularQueue(value);
        traState.update(tra);
        if(tra.id != -1) {
            tra = SerializationUtils.clone(tra);
            QueryTraInfo queryTraInfo = new QueryTraInfo(tra, queryInfoValueState.value());
            out.collect(queryTraInfo);
        }
    }

    @Override
    public void processElement2(QueryInfo info, KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        TracingQueue tra = traState.value();
        if (tra.id == -1) {
            tra.updateId(info.queryTraId);
            queryInfoValueState.update(info);
            System.out.println("invalid query");
        }
    }
}
