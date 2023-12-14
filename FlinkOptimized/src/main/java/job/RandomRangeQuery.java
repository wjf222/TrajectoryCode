package job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.stream.DoubleStream;

public class RandomRangeQuery {
    public static void main(String[] args) throws IOException {
        int queryNum = 10;
        File aggFile = new File("range_generate_auto.txt");
        aggFile.createNewFile();
        FileWriter aggWriter = new FileWriter(aggFile);
        Random random = new Random();
        DoubleStream doublesX = random.doubles(queryNum, 116.0, 116.8);
        DoubleStream doublesX2 = random.doubles(queryNum, 116.0, 116.8);
        DoubleStream doublesY = random.doubles(queryNum, 39.5, 40.3);
        DoubleStream doublesY2 = random.doubles(queryNum, 39.5, 40.3);
        double[] randomDoublesX = doublesX.toArray();
        double[] randomDoublesX2 = doublesX2.toArray();
        double[] randomDoublesY = doublesY.toArray();
        double[] randomDoublesY2 = doublesY2.toArray();
        for(int i = 0; i < queryNum;i++) {
            double xlo = Math.min(randomDoublesX[i],randomDoublesX2[i]);
            double ylo = Math.min(randomDoublesY[i],randomDoublesY2[i]);
            double xHi = Math.max(randomDoublesX[i],randomDoublesX2[i]);
            double yHi = Math.max(randomDoublesY[i],randomDoublesY2[i]);
            aggWriter.write(String.format("%f,%f,%f,%f\r\n",xlo,ylo,xHi,yHi));
        }
        aggWriter.flush();
        aggWriter.close();
    }
}
