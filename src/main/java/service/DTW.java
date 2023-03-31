package service;

import objects.TracingPoint;
import util.PointTool;


public class DTW implements Similarity{
    public double oneDimensional(double[] p1, double[] p2) {
        int r = p1.length;
        int c = p2.length;
        double[][] D0 = new double[r+1][c+1];
        for(int i = 0; i < r;i++){
            for(int j = 0; j < c;j++){
                D0[i][j] = Math.abs(p1[i] - p2[j]);
            }
        }
        return calculation(D0);
    }
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second){
        int r = first.length;
        int c = second.length;
        double[][] D0 = new double[r+1][c+1];
        for(int i = 0; i < r;i++){
            for(int j = 0; j < c;j++){
                D0[i][j] = Math.abs(PointTool.getDistance(first[i].longitude,first[i].latitude,second[j].longitude,second[j].latitude));
            }
        }
        return calculation(D0);
    }

    private double calculation(double[][] x){
        int r = x.length-1;
        int c = x[0].length-1;
        double[][] D = new double[r][c];
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                D[i][j] = x[i + 1][j + 1];
            }
        }

//        System.out.println("距离矩阵D:");
//        System.out.println(Arrays.deepToString(D).replaceAll("],", "]," + System.getProperty("line.separator")));

        //计算损失矩阵M
        double[][] MC = x.clone();
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                if(i == 0 && j != 0) {
                    MC[i][j] += MC[i][j-1];
                } else if(i != 0 && j == 0){
                    MC[i][j] += MC[i-1][j];
                } else if(i != 0 && j != 0){
                    MC[i][j] += Math.min(Math.min(MC[i - 1][j - 1], MC[i][j - 1]), MC[i - 1][j]);
                }
            }
        }
        return MC[r - 1][c - 1];
    }
}
