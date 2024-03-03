package job;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomQuery {
    public static void main(String[] args) throws IOException {
        int queryNum = 100;
        int trajectoryIdMax = 182;
        File aggFile = new File("geolife-sim-query_auto.txt");
        aggFile.createNewFile();
        FileWriter aggWriter = new FileWriter(aggFile);
        Random random = new Random();
        for(int i = 0; i < queryNum;i++) {
            int id = random.nextInt(trajectoryIdMax)+1;
            int threshold = random.nextInt(trajectoryIdMax);
            aggWriter.write(String.format("%d,%d\r\n",id,threshold));
        }
        aggWriter.flush();
        aggWriter.close();
    }
}
