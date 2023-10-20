package service.similarity;

import pojo.TracingPoint;
import util.PointTool;


public class DTW implements Similarity {
    @Override
    public double compute(TracingPoint[] first, TracingPoint[] second){
        int r = first.length;
        int c = second.length;
        double[][] D0 = new double[r+1][c+1];
        for(int i = 0; i < r;i++){
            for(int j = 0; j < second.length;j++){
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
            System.arraycopy(x[i + 1], 1, D[i], 0, c);
        }

        //计算损失矩阵M
        double[][] MC = x.clone();
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                if(i == 0 && j != 0) {
                    MC[i][j] += MC[i][j-1];
                } else if(i != 0 && j == 0){
                    MC[i][j] += MC[i-1][j];
                } else if(i != 0){
                    MC[i][j] += Math.min(Math.min(MC[i - 1][j - 1], MC[i][j - 1]), MC[i - 1][j]);
                }
            }
        }
        return MC[r - 1][c - 1];
    }
}
