package cn.edu.tsinghua.tsquality.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.rank.Median;

public class Util {

    public static String formatTimestamp(long timestamp) {
        LocalDateTime time =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        return time.format(formatter);
    }

    public static double[] toDoubleArray(List<Double> list) {
        return list.stream().mapToDouble(Double::valueOf).toArray();
    }

    public static double[] variation(double[] origin) {
        int n = origin.length;
        double[] variance = new double[n - 1];
        for (int i = 0; i < n - 1; ++i) {
            variance[i] = origin[i + 1] - origin[i];
        }
        return variance;
    }

    public static double[] speed(double[] origin, double[] time) {
        int n = origin.length;
        double[] speed = new double[n - 1];
        for (int i = 0; i < n - 1; ++i) {
            speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
        }
        return speed;
    }

    public static double mad(double[] value) {
        Median median = new Median();
        double mid = median.evaluate(value);
        double[] d = new double[value.length];
        for (int i = 0; i < value.length; ++i) {
            d[i] = Math.abs(value[i] - mid);
        }
        return 1.4826 * median.evaluate(d);
    }

    public static int findOutliers(double[] value, double k) {
        Median median = new Median();
        double mid = median.evaluate(value);
        double sigma = Util.mad(value);
        int num = 0;
        for (double v : value) {
            if (Math.abs(v - mid) > k * sigma) {
                ++num;
            }
        }
        return num;
    }
}
