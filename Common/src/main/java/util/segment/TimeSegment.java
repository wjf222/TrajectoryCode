package util.segment;

import entity.TracingPoint;

import java.util.ArrayList;
import java.util.List;

public class TimeSegment implements Segment{

    private int timeLimit;

    public TimeSegment(){
        this.timeLimit = 10;
    }
    public TimeSegment(int timeLimit) {
        this.timeLimit = timeLimit;
    }
    @Override
    public List<List<TracingPoint>> processing(Iterable<TracingPoint> points) {
        List<List<TracingPoint>> ret = new ArrayList<>();
        List<TracingPoint> queue = new ArrayList<>();
        for(TracingPoint point:points) {
            if(queue.size() < timeLimit) {
                queue.add(point);
            }else {
                ret.add(queue);
                queue = new ArrayList<>();
                queue.add(point);
            }
        }
        if(queue.size() != 0) {
            ret.add(queue);
        }
        return ret;
    }
}
