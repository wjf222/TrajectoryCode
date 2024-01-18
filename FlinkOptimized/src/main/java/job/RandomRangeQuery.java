package job;

import entity.TracingPoint;
import entity.WindowPoint;
import indexs.commons.Window;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.DoubleStream;

public class RandomRangeQuery {
    public static void main(String[] args) throws IOException {
        int queryNum = 100;
        int numVertices = 10;
        File aggFile = new File("range_generate_auto.txt");
        aggFile.createNewFile();
        FileWriter aggWriter = new FileWriter(aggFile);
        Random random = new Random();
        for(int i = 0; i < queryNum;i++) {
            Window window = generateRandomPolygon(numVertices);
            List<WindowPoint> pointList = window.getPointList();
            aggWriter.write(
                    String.format("%f,%f,%f,%f;%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\r\n"
                            ,window.getXmin(),window.getYmin(),window.getXmax(),window.getYmax()
                            ,pointList.get(0).getX(),pointList.get(0).getY(),pointList.get(1).getX(),pointList.get(1).getY()
                            ,pointList.get(2).getX(),pointList.get(2).getY(),pointList.get(3).getX(),pointList.get(3).getY()
                            ,pointList.get(4).getX(),pointList.get(4).getY(),pointList.get(5).getX(),pointList.get(5).getY()
                            ,pointList.get(6).getX(),pointList.get(6).getY(),pointList.get(7).getX(),pointList.get(7).getY()
                            ,pointList.get(8).getX(),pointList.get(8).getY(),pointList.get(9).getX(),pointList.get(9).getY()));
        }
        aggWriter.flush();
        aggWriter.close();
    }

    private static Window generateRandomPolygon(int numVertices) {
        List<WindowPoint> points = new ArrayList<>();
        Random random = new Random();
        DoubleStream doublesX = random.doubles(numVertices, 116.0, 116.8);
        DoubleStream doublesY = random.doubles(numVertices, 39.5, 40.3);
        double[] randomDoublesX = doublesX.toArray();
        double[] randomDoublesY = doublesY.toArray();
        for (int i = 0; i < numVertices; i++) {
            double x = randomDoublesX[i];
            double y = randomDoublesY[i];
            points.add(new WindowPoint(x, y));
        }
        Arrays.sort(randomDoublesX);
        Arrays.sort(randomDoublesY);
        Window window = new Window(randomDoublesX[0],randomDoublesY[0],randomDoublesX[numVertices-1],randomDoublesY[numVertices-1]);
        // 排序点以形成一个闭环
        Collections.sort(points, (a, b) -> Double.compare(a.getX(), b.getX()));
        window.setPointList(points);
        return window;
    }
}
