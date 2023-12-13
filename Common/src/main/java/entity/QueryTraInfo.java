package entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class QueryTraInfo implements Serializable {
    public TracingQueue queryTra;
    public QueryInfo info;
    public long anotherTraId = -1;
    public QueryTraInfo(){}
    public QueryTraInfo(TracingQueue tra, QueryInfo info) {
        this.queryTra = tra;
        this.info = info;
    }
    public QueryTraInfo(QueryTraInfo tra,long anotherTraId) {
        this.queryTra = tra.queryTra;
        this.info = tra.info;
        this.anotherTraId = anotherTraId;
    }
}
