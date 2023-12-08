package operator;

import entity.QueryPair;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class XZFilter extends RichFilterFunction<QueryPair> {
    @Override
    public boolean filter(QueryPair pair) throws Exception {
        return false;
    }
}
