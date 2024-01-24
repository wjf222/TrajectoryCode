package com.wjf.trajectory.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RangeQueryResult {
    private String query;
    private String result;
    private int count;
}
