package entity;

public class TrajectoryIdPair {
    private long trajectoryId;
    private long anotherId;

    public TrajectoryIdPair(long trajectoryId,long anotherId) {
        this.trajectoryId = trajectoryId;
        this.anotherId = anotherId;
    }
    @Override
    public int hashCode() {
        int code = 17;
        code = 31*code + (int)(trajectoryId ^ (trajectoryId >>> 32));
        code = 31*code + (int)(anotherId ^ (anotherId >>> 32));
        return code;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {  //测试检测的对象是否为空，是就返回false
            return true;
        }
        if(obj == null) {  //测试两个对象所属的类是否相同，否则返回false
            return false;
        }
        if(getClass() != obj.getClass()) {  //对otherObject进行类型转换以便和类A的对象进行比较
            return false;
        }
        TrajectoryIdPair another = (TrajectoryIdPair) obj;
        return trajectoryId == another.trajectoryId && anotherId == another.anotherId;
    }
}
