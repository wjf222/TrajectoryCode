package util;

import entity.TracingPoint;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

public class ParamHelper {
    private static ParameterTool paramTool;
    public static void initFromArgs(String[] args) {
        if(args != null && args.length != 0) {
            ParameterTool argParameter = ParameterTool.fromArgs(args);
            String propertiesFilePath = argParameter.getRequired("ConfigFile");
            try {
                paramTool = ParameterTool.fromPropertiesFile(propertiesFilePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            paramTool = paramTool.mergeWith(argParameter);
        }
        System.out.println("============Overall Params============");
        paramTool.toMap().forEach((k, v) -> System.out.println(k + ": " + v));
        System.out.println("======================================");
    }

    public static String getDataPath() {
        return paramTool.getRequired("data_path");
    }

    public static int getDataSize() {
        return paramTool.getInt("data_size");
    }
    public static String getQueryPath() {
        return paramTool.getRequired("query_path");
    }

    public static long getTimeWindowSize() {
        return paramTool.getLong("time_window");
    }

    public static int getDistMeasure() {
        return paramTool.getInt("dist_measure");
    }

    public static double getLCSSThreshold() {
        return paramTool.getDouble("lcss_threshold");
    }

    public static int getLCSSDelta() {
        return paramTool.getInt("lcss_delta");
    }

    public static double getEDRThreshold() {
        return paramTool.getDouble("edr_threshold");
    }

    public static TracingPoint getERPGap() {
        double x = paramTool.getDouble("erp_gap_x");
        double y = paramTool.getDouble("erp_gap_y");
        return new TracingPoint(x, y, -1, -1);
    }
    public static long getDelayReduceTime() {
        return paramTool.getLong("delay_reduce");
    }
    public static String getSinkDir() {
        return paramTool.getRequired("sink_dir");
    }

    public static int getContinuousQueryNum() {
        return paramTool.getInt("continuous_query_num");
    }
}
