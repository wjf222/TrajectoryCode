package com.wjf.trajectory.common.indexs.commons;

/**
 * 由于范围查询需要提供访问的四个端点
 * 但是要求四个端点为正则化之后的结果
 * @author 王纪锋
 */
public class QueryWindow extends Window{
    public QueryWindow(double xmin, double ymin, double xmax, double ymax) {
        super(xmin, ymin, xmax, ymax);
    }
}
