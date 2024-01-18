package com.wjf.trajectory.FlinkBase.operator.similarity;

import entity.QueryInfo;
import entity.QueryTraInfo;
import entity.TracingPoint;
import entity.TracingQueue;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class QueryTraInfoGenerator extends KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo> {

    public long timeWindow;
    public int continuousQueryNum;
    public int query_size;
    // 轨迹状态
    private ValueState<TracingQueue> traState;
    private ValueState<QueryInfo> queryInfoValueState;
    private ValueState<Integer> queryNum;
    public  QueryTraInfoGenerator(long timeWindow,int continuousQueryNum,int query_size) {
        this.timeWindow = timeWindow;
        this.continuousQueryNum = continuousQueryNum;
        this.query_size = query_size;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        traState = getRuntimeContext()
                .getState(new ValueStateDescriptor<TracingQueue>("traState",TracingQueue.class,new TracingQueue(timeWindow)));
        queryInfoValueState = getRuntimeContext()
                .getState(new ValueStateDescriptor<QueryInfo>("queryInfoValueState",QueryInfo.class,new QueryInfo()));
        queryNum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("queryNum",Integer.class,new Integer(0)));
    }

    @Override
    public void processElement1(TracingPoint value, KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        TracingQueue tra = traState.value();
        tra.EnCircularQueue(value);
        tra.id = tra.id == -1 ? value.id:tra.id;
        traState.update(tra);
        long queryInfoId = queryInfoValueState.value().queryTraId;
        int queryNumAccumulator = queryNum.value();
        if(queryInfoId != -1 && queryNumAccumulator < this.continuousQueryNum && tra.queueArray.size() > query_size  ) {
            tra = SerializationUtils.clone(tra);
            QueryTraInfo queryTraInfo = new QueryTraInfo(tra, queryInfoValueState.value());
            out.collect(queryTraInfo);
            queryNum.update(queryNumAccumulator+1);
        }
    }

    @Override
    public void processElement2(QueryInfo info, KeyedCoProcessFunction<Long, TracingPoint, QueryInfo, QueryTraInfo>.Context ctx, Collector<QueryTraInfo> out) throws Exception {
        TracingQueue tra = traState.value();
        // 初始化 轨迹Id,查询ID,查询次数归零
        tra.updateId(info.queryTraId);
        traState.update(tra);
        queryInfoValueState.update(info);
        queryNum.update(1);
        // 如果查询轨迹已经初始化完成
        if(!tra.queueArray.isEmpty() && tra.queueArray.size() > query_size) {
            QueryTraInfo queryTraInfo = new QueryTraInfo(tra, info);
            out.collect(queryTraInfo);
        }
    }
}