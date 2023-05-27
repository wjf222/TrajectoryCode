
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class F13partitionCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dss = env.fromSequence(1,10);
        // 自定义分区：将 奇数 发给 0号分区，将 偶数 发给 1号分区
        DataStream<Long> pcDS = dss.partitionCustom(new Partitioner<Long>() {
            @Override
            public int partition(Long key, int numPartitions) {
                System.out.println("total partition num:"+numPartitions);
                if(key%2 == 0){
                    return 1;
                }else {
                    return 0;
                }
            }
        }, (KeySelector<Long, Long>) value -> value);
        // 重组数据，第一个元素是分区索引，第二个元素是原数据
        SingleOutputStreamOperator<Tuple2<Integer, Long>> map = pcDS.map(new RichMapFunction<Long, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(Long value) throws Exception {
                return Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), value);
            }
        });
        map.print();
        System.out.println("执行计划："+env.getExecutionPlan());
        env.execute();
    }
}
