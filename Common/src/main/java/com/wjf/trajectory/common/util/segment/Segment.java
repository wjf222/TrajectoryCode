package com.wjf.trajectory.common.util.segment;

import com.wjf.trajectory.common.entity.TracingPoint;

import java.util.List;

public interface Segment {
    List<List<TracingPoint>> processing(Iterable<TracingPoint> points);
}
