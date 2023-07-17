package indexs.commons;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 由于范围查询需要提供访问的四个端点
 * 因此提供本类抽象四个端点
 * @author 王纪锋
 */
@Data
@AllArgsConstructor
public class Window {
    private double xmin;
    private double ymin;
    private double xmax;
    private double ymax;
}