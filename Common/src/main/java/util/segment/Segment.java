package util.segment;

import entity.TracingPoint;

import java.util.Iterator;
import java.util.List;

public interface Segment {
    List<List<TracingPoint>> processing(Iterable<TracingPoint> points);
}
