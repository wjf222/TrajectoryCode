package job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class RepeatLines {
    public static void main(String[] args) {
        String inputFilePath = "D:\\opensource\\wjf\\Config\\data-sorted.txt"; // 输入文件路径
        String outputFilePath = "D:\\opensource\\wjf\\Config\\data-sorted4.txt"; // 输出文件路径
        int N = 4; // 每行重复的次数

        // 使用try-with-resources语句自动管理资源
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {

            String line;

            while ((line = reader.readLine()) != null) {
                writer.println(line);
                for (int i = 1; i < N; i++) {
                    String tmpLine = String.valueOf(i)+line;
                    writer.println(tmpLine);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
