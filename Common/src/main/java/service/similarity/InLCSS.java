package service.similarity;

import entity.TracingPoint;
import entity.TracingQueue;
import entity.TrajectoryIdPair;
import service.Similarity;
import util.PointTool;

import java.util.*;

public class InLCSS implements Similarity {
    private static final ThreadLocal<Map<TrajectoryIdPair, List<Double>>> threadLastResult = ThreadLocal.withInitial(() -> new HashMap<>());
    @Override
    public double compute(TracingQueue firstTrajectory, TracingQueue queryTrajectory) {
        Map<TrajectoryIdPair, List<Double>> trajectoryIdPairListMap = threadLastResult.get();
        TrajectoryIdPair trajectoryIdPair = new TrajectoryIdPair(firstTrajectory.getId(), queryTrajectory.getId());
        List<Double> lastResult = trajectoryIdPairListMap.getOrDefault(trajectoryIdPair,new ArrayList<>());

        Deque<TracingPoint> first = firstTrajectory.queueArray;
        Deque<TracingPoint> second = queryTrajectory.queueArray;
        TracingPoint source = first.peekLast();
        if(source == null) {
            throw new RuntimeException(String.format("firstTrajectory is null:\r\n first.length:%d\tsecond.length:%d\t",first.size(),second.size()));
        }
        int secondLength = second.size();
        double[] result = new double[secondLength+1];
        TracingPoint[] secondTrace = second.toArray(new TracingPoint[0]);
        for(int j = 0; j < secondLength;j++){
            if(PointTool.getDistance(source,secondTrace[j]) < 50){
                result[j + 1] = getLastResult(lastResult,j) + 1;
            } else {
                result[j + 1] = Math.max(getLastResult(lastResult,j+1),result[j]);
            }
        }
        lastResult = setLastResult(result);
        trajectoryIdPairListMap.put(trajectoryIdPair,lastResult);
        return result[secondLength];
    }

    private double getLastResult(List<Double> lastResult,int i) {
        if(i >= lastResult.size()){
            return 0;
        }
        return lastResult.get(i);
    }

    private List<Double> setLastResult(double[] result) {
        List<Double> lastResult = new ArrayList<>(result.length+1);
        for(double dist:result) {
            lastResult.add(dist);
        }
        return lastResult;
    }
}
