package entity;

import indexs.commons.Window;
import lombok.Data;
@Data
public class RangeQueryPair {
    TracingQueue tracingQueue;
    Window window;
    public boolean contain = false;
    public long startTimestamp = Long.MAX_VALUE;
    public long endTimestamp = 0;
}
