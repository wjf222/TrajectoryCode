package com.wjf.trajectory.common.indexs.z2;

import com.wjf.trajectory.common.indexs.IndexRange;
import com.wjf.trajectory.common.indexs.commons.QueryWindow;
import com.wjf.trajectory.common.indexs.commons.Window;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 空间扩展填充Z曲线  XZ2 : 用户存储线线、面数据
 * 基于 'XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension'
 * SFC：空间填充曲线缩写
 * @author 王纪锋
 */
@Data
public class XZ2SFC implements Serializable {

    /**
     * 初始四划分
     */
    private List<XElement> LevelOneElements = new XElement(0.0,0.0,1.0,1.0,1.0).children();

    /**
     * 代表搜索了整个四叉树或者八叉树
     */
    private XElement LevelTerminator = new XElement(-1.0,-1.0,-1.0,-1.0,0);

    /**
     * 缓存XZ2SFC
     */
    private static ConcurrentHashMap<Short,XZ2SFC> cache = new ConcurrentHashMap<>();

    private static XZ2SFC getXZ2SFC(short g) {
        // TODO 实现多线程安全的缓存
        XZ2SFC xz2SFC = cache.get(g);
        if (xz2SFC == null) {
            xz2SFC = new XZ2SFC(g,-180.0,180.0,-90.0,90.0);
            cache.put(g,xz2SFC);
        }
        return xz2SFC;
    }


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
        Bounding normal_bound = normalize(xmin,ymin,xmax,ymax);

        // 计算这一段线性序列的长度,由于是正方形格网，所以应该选择X,Y中的较长的一段
        double maxDim = Math.max(normal_bound.xHi-normal_bound.xLo,normal_bound.yHi-normal_bound.yLo);

        // 象限序列的最小长度为l1, l1 <= s < l2 ; l2 = l1+2;
        int l1 = (int) Math.floor(Math.log(maxDim)/Math.log(0.5));

        int length = resolution;
        if(l1 < resolution) {
            // 元素在l2分辨率下的宽度
            double w2 = Math.pow(0.5,l1+1);
            // 检查多边形与多少个轴相交
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
     * 实现窗口查询，将查询语句正则化之后进行处理
     * @param queries 查询窗口（可能未标准化）
     * @param maxRanges 生成的范围上限
     * @return
     */
    public List<IndexRange> ranges(List<Window> queries, Optional<Integer> maxRanges){
        QueryWindow[] QueryWindow = queries.stream().map(query -> {
            Bounding bounding = normalize(query.getXmin(), query.getYmin(), query.getXmax(), query.getYmax());
            return new QueryWindow(bounding.xLo, bounding.yLo, bounding.yHi, bounding.yHi);
        }).toArray(com.wjf.trajectory.common.indexs.commons.QueryWindow[]::new);
        return ranges(QueryWindow,maxRanges.orElse(Integer.MAX_VALUE));
    }

    /**
     * 查询给定范围下的XZ曲线覆盖范围
     * @param queries 正则化后的窗口
     * @param rangeStop 生成的范围数量的上限
     * @return
     */
    private List<IndexRange> ranges(QueryWindow[] queries,int rangeStop) {
        // 存储结果
        List<IndexRange> ranges = new ArrayList<>(100);
        // 等待处理的数据
        Deque<XElement> remaining = new ArrayDeque<>(100);
        // 初始化
        LevelOneElements.forEach(elements -> remaining.add(elements));
        remaining.add(LevelTerminator);
        short level = 1;
        while (level < resolution && !remaining.isEmpty() && ranges.size() < rangeStop) {
            XElement next = remaining.poll();
            if (next.equals(LevelTerminator)) {
                // 当前level处理完毕
                if(!remaining.isEmpty()) {
                    level = (short) (level+ 1);
                    remaining.add(LevelTerminator);
                }
            } else {
                if(next.isContained(queries)) {
                    Tuple2<Long, Long> tuple2 = sequenceInterval(next.xmin, next.ymin, level, false);
                    ranges.add(new IndexRange(tuple2.getField(0),tuple2.getField(1),true));
                } else if (next.isOverlapped(queries)) {
                    Tuple2<Long, Long> tuple2 = sequenceInterval(next.xmin, next.ymin, level, true);
                    ranges.add(new IndexRange(tuple2.getField(0),tuple2.getField(1),false));
                    next.children().forEach(xElement -> remaining.add(xElement));
                }
            }
        }
        // 处理剩余的数据
        while (!remaining.isEmpty()) {
            XElement quad = remaining.poll();
            if (quad.equals(LevelTerminator)) {
                level = (short) (level+1);
            } else {
                Tuple2<Long, Long> tuple2 = sequenceInterval(quad.xmin, quad.ymin, level, false);
                ranges.add(new IndexRange(tuple2.getField(0),tuple2.getField(1),false));
            }
        }
        // 获取到了所有的Range,通过合并减少重叠的范围
        ranges.sort(new Comparator<IndexRange>() {
            @Override
            public int compare(IndexRange x, IndexRange y) {
                long c1 = x.lower - y.lower;
                if(c1 != 0) return (int) c1;
                long c2 = x.upper - y.upper;
                if(c2 != 0) return (int) c2;
                return 0;
            }
        });
        // 至少有一个返回值
        IndexRange current = ranges.get(0);
        List<IndexRange> result = new ArrayList<>();
        int i = 1;
        while (i < ranges.size()) {
            IndexRange range = ranges.get(i);
            if (range.lower <= current.upper + 1) {
                // 合并两个range
                current = new IndexRange(current.lower,Math.max(current.upper,range.upper),current.contained && range.contained);
            } else {
                result.add(current);
                current = range;
            }
            i++;
        }
        result.add(current);
        return result;
    }
    /**
     * 将用户空间归一化
     * @param xmin 用户空间内的X最小值
     * @param ymin 用户空间内的y最小值
     * @param xmax 用户空间内的x最大值
     * @param ymax 用户空间内的y最大值
     * @return 归一化后的用户包围圈
     */
    private Bounding normalize(double xmin, double ymin, double xmax, double ymax) {
        if(xmin > xmax || ymin > ymax) {
            throw new IllegalArgumentException("Bounds must be ordered: xmin < xmax && ymin < ymax");
        }
        double nxmin = (xmin - bounding.xLo) /xSize;
        double nymin = (ymin - bounding.yLo) / ySize;
        double nxmax = (xmax - bounding.xLo) / xSize;
        double nymax = (ymax - bounding.yLo) / ySize;
        return new Bounding(nxmin, nxmax, nymin, nymax);
    }
    private Tuple2<Long, Long> sequenceInterval(double x, double y, short length, boolean partial) {
        long min = sequenceCode(x, y, length);
        long max = min + (Double.valueOf(Math.pow(4, resolution - length + 1)).longValue() - 1L)/3L;
        max = partial?min:max;
        return new Tuple2<>(min, max);
    }

    /**
     * 一个包含了坐标边界的包围盒
     */
    @AllArgsConstructor
    @Data
    public static class Bounding implements Serializable{
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

    /**
     * 扩展Z曲线元素。为了简化计算，边界是指非扩展的Z元素
     * 扩展Z元素是指一个正常的Z元素，它的上界扩展了它的宽度和高度的两倍，按照惯例，元素是正方形
     */
    @Data
    public static class XElement implements Serializable{
        /**
         * 扩展后的X边界
         */
        private double xext;
        /**
         * 扩展后的Y边界
         */
        private double yext;

        private double xmin;
        private double ymin;
        private double xmax;
        private double ymax;
        private double length;
        /**
         * 标准化后的元素边界
         * @param xmin [0,1]
         * @param ymin [0,1]
         * @param xmax [0,1] ,大于xmin
         * @param ymax [0,1] ,大于ymin
         * @param length 非延伸边长度，通常长度和高度相等，为正方形
         */
        public XElement(double xmin, double ymin, double xmax, double ymax, double length) {
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
            this.length = length;
            this.xext = xmax+length;
            this.yext = ymax+length;
        }

        /**

         * @param window 目标窗口
         * @return 当前元素是否被包含
         */
        private boolean isContained(QueryWindow window) {
            return window.getXmin() <= xmin && window.getYmin() <= ymin && window.getXmax() >= xext && window.getYmax() >= yext;
        }

        /**
         * @param windows 所有待搜索的空间
         * @return 当前元素是否被包含在搜索空间内
         */
        private boolean isContained(QueryWindow[] windows) {
            for(QueryWindow window:windows) {
                if(this.isContained(window)) {
                    return true;
                }
            }
            return false;
        }


        /**
         * @param window 目标窗口
         * @return 当前元素与窗口是否相交
         */
        private boolean overlaps(QueryWindow window) {
            return window.getXmax() >=xmin && window.getYmax() >= ymin && window.getXmin() <= xext && window.getYmin() <= yext;
        }


        /**
         * @param windows 目标窗口
         * @return 当前元素是否被包含在搜索空间中的某部分相交
         */
        private boolean isOverlapped(QueryWindow[] windows) {
            for(QueryWindow window:windows) {
                if(this.overlaps(window)) {
                    return true;
                }
            }
            return false;
        }
        private List<XElement> children() {
            double xCenter = (xmin + xmax) / 2.0;
            double yCenter = (ymin + ymax) / 2.0;
            double len = length / 2.0;
            List<XElement> ans = new ArrayList<>();
            ans.add(new XElement(xmin,ymin,xCenter,yCenter,len));
            ans.add(new XElement(xCenter,ymin,xmax,yCenter,len));
            ans.add(new XElement(xmin,yCenter,xCenter,ymax,len));
            ans.add(new XElement(xCenter,yCenter,xmax,ymax,len));
            return ans;

        }
    }
}
