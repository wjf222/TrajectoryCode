package com.wjf.trajectory.FlinkBase.operator.similarity;

import entity.QueryInfo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class QueryInfoLoader extends KeyedProcessFunction<Integer,String, QueryInfo> {
    private ListState<QueryInfo> queryInfoList;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queryInfoList = getRuntimeContext().getListState(new ListStateDescriptor<>("queryInfoList", QueryInfo.class));
    }

    @Override
    public void processElement(String queryLine, KeyedProcessFunction<Integer, String, QueryInfo>.Context ctx, Collector<QueryInfo> out) throws Exception {
        if (queryLine != null && !queryLine.equals("")) {
            String[] words = queryLine.split(",");
            queryInfoList.add(new QueryInfo(Long.parseLong(words[0]), Double.parseDouble(words[1])));
        }
        queryInfoList.get().forEach(out::collect);
        queryInfoList.clear();
    }
}
