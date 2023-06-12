package indexs.z2;

import lombok.AllArgsConstructor;

/**
 * 空间扩展填充Z曲线  XZ2 : 用户存储线线、面数据
 * 基于 'XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension'
 */
public class XZ2SFC {

    private Bounding bounding;
    /**
     * 空间将被递归四划分的次数
     */
    private short resolution;

    private double xSize;
    private double ySize;
    public XZ2SFC(short r,double xLo,double xHi,double yLo,double yHi) {
        this.resolution = r;
        this.bounding = new Bounding(xLo, xHi, yLo, yHi);
        xSize = xHi - xLo;
        ySize = yHi - yLo;
    }

    /**
     * 基于包围盒的 Index
     * @param xmin 包围盒中的X最小值
     * @param ymin 包围盒中的Y最小值
     * @param xmax 包围盒中的X最大值
     * @param ymax 包围盒中的Y最大值
     * @param lenient 是否标准化为一个有效值
     * @return
     */
    public long index(double xmin,double ymin,double xmax,double ymax, boolean lenient) {
        Bounding normal_bound = normalize(xmin,ymin,xmax,ymax,lenient);

        // 计算这一段线性序列的长度,由于是正方形格网，所以应该选择X,Y中的较长的一段
        double maxDim = Math.max(normal_bound.xHi-normal_bound.xLo,normal_bound.yHi-normal_bound.yLo);

        // 象限序列的最小长度为l1, l1 <= s < l2 ; l2 = l1+2;
        int l1 = (int) Math.floor(Math.log(maxDim)/Math.log(0.5));

        int length = resolution;
        if(l1 < resolution) {
            // 元素在l2分辨率下的宽度
            double w2 = Math.pow(0.5,l1+1);
            // 检查多边形与多少个轴香蕉
            boolean pre_x = normal_bound.xHi <= ((Math.floor(normal_bound.xLo / w2) * w2) + (2 * w2));
            boolean pre_y = normal_bound.yHi <= ((Math.floor(normal_bound.yLo / w2) * w2) + (2 * w2));
            length = pre_y&&pre_x?l1+1:l1;
        }
        return sequenceCode(normal_bound.xLo,normal_bound.yLo,length);
    }


    private long sequenceCode(double x,double y,int length) {
        double xmin = 0.0;
        double ymin = 0.0;
        double xmax = 1.0;
        double ymax = 1.0;

        long seqCode = 0L;
        for(int i = 0; i < length;i++) {
            double xCenter = (xmin + xmax) /2.0;
            double yCenter = (ymin + ymax) /2.0;
            if(x < xCenter && y < yCenter) {
                seqCode += 1L;
                xmax = xCenter;
                ymax = yCenter;
            } else if(!(x < xCenter) && y < yCenter) {
                seqCode += 1L + 1L*((long) Math.pow(4,resolution-i)-1L)/3L;
                xmin = xCenter;
                ymax = yCenter;
            } else if (x < xCenter && !(y<yCenter)) {
                seqCode += 1L + 2L*((long) Math.pow(4,resolution-i)-1L)/3L;
                xmax = xCenter;
                ymin = yCenter;
            } else if(!(x < xCenter) && !(y<yCenter)){
                seqCode += 1L + 3L*((long) Math.pow(4,resolution-i)-1L)/3L;
                xmin = xCenter;
                ymin = yCenter;
            }
        }
        return seqCode;
    }


    /**
     * 将用户空间归一化
     * @param xmin 用户空间内的X最小值
     * @param ymin 用户空间内的y最小值
     * @param xmax 用户空间内的x最大值
     * @param ymax 用户空间内的y最大值
     * @param lenient 是否修正异常
     * @return 归一化后的用户包围圈
     */
    private Bounding normalize(double xmin,double ymin,double xmax,double ymax,boolean lenient) {
        if(xmin > xmax || ymin > ymax) {
            throw new IllegalArgumentException("Bounds must be ordered: xmin < xmax && ymin < ymax");
        }
        double nxmin = (xmin - bounding.xLo) /xSize;
        double nymin = (ymin - bounding.yLo) / ySize;
        double nxmax = (xmax - bounding.xLo) / xSize;
        double nymax = (ymax - bounding.yLo) / ySize;
        return new Bounding(nxmin, nxmax, nymin, nymax);
    }
    /**
     * 一个包含了坐标边界的包围盒
     */
    @AllArgsConstructor
    private static class Bounding{
        /**
         * X 下界
         */
        private double xLo;
        /**
         * X 下界
         */
        private double xHi;
        /**
         * y 下界
         */
        private double yLo;
        /**
         * y 上界
         */
        private double yHi;
    }
}
