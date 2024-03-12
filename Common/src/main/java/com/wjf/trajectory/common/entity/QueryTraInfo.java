package com.wjf.trajectory.common.entity;

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

    @Override
    public int hashCode(){
        return (int) info.queryTraId;
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(obj == null){
            return false;
        }
        if(!(obj instanceof QueryTraInfo)){
            return false;
        }
        QueryTraInfo a = (QueryTraInfo) obj;
        return a.queryTra.id == this.queryTra.id;
    }
}
