package indexs;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
/**
 * 范围上下界
 */
public class IndexRange {
    public long lower;
    public long upper;
    public boolean contained;

    public boolean intersect(long index){
        return index >= lower && index <= upper;
    }
}
class CoveredRange extends IndexRange {
    public CoveredRange(long lower,long upper){
        this.lower = lower;
        this.upper = upper;
        this.contained = true;
    }
}

class OverlappingRange extends IndexRange {
    public OverlappingRange(long lower,long upper){
        this.lower = lower;
        this.upper = upper;
        this.contained = false;
    }
}