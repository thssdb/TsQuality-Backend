package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionRequest;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Util {
    // split time series path to {device}.{sensor}
    public static String[] splitSeriesPath(String seriesPath) {
        String[] result = new String[2];
        int index = seriesPath.lastIndexOf('.');
        if (index == -1) {
            result[0] = seriesPath;
            result[1] = "";
        } else {
            result[0] = seriesPath.substring(0, index);
            result[1] = seriesPath.substring(index + 1);
        }
        return result;
    }

    public static String formatTimestamp(long timestamp) {
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
        return time.format(formatter);
    }

    public static String constructQuerySQL(String seriesPath, IoTDBSeriesAnomalyDetectionRequest request) {
        IoTDBSeriesAnomalyDetectionRequest.TimeFilter timeFilter = request.getTimeFilter();
        IoTDBSeriesAnomalyDetectionRequest.ValueFilter valueFilter = request.getValueFilter();
        return constructQuerySQL(
                seriesPath,
                timeFilter == null ? 0 : timeFilter.getMinTimestamp(),
                timeFilter == null ? 0 : timeFilter.getMaxTimestamp(),
                valueFilter == null ? "" : valueFilter.getContent());
    }

    public static String constructQuerySQL(
            String seriesPath, long minTimeFilter, long maxTimeFilter, String valueFilter
    ) {
        String[] splitRes = splitSeriesPath(seriesPath);
        String device = splitRes[0];
        String sensor = splitRes[1];
        String sql = String.format("SELECT %s FROM %s", sensor, device);
        if (minTimeFilter != 0) {
            sql += String.format(" WHERE time > %d", minTimeFilter);
        }
        if (maxTimeFilter != 0) {
            if (minTimeFilter != 0) {
                sql += " AND";
            }
            sql += String.format(" time < %d", maxTimeFilter);
        }
        if (valueFilter != null && !valueFilter.isEmpty()) {
            if (minTimeFilter != 0 || maxTimeFilter != 0) {
                sql += " AND";
            }
            sql += String.format(" %s %s", sensor, valueFilter);
        }
        return sql;
    }

    public static double[] toDoubleArray(List<Double> list) {
        return list.stream().mapToDouble(Double::valueOf).toArray();
    }

    public static double[] variation(double[] origin) {
        int n = origin.length;
        double[] variance = new double[n - 1];
        for(int i = 0; i < n - 1; ++i) {
            variance[i] = origin[i + 1] - origin[i];
        }
        return variance;
    }

    public static double[] speed(double[] origin, double[] time) {
        int n = origin.length;
        double[] speed = new double[n - 1];
        for(int i = 0; i < n - 1; ++i) {
            speed[i] = (origin[i + 1] - origin[i]) / (time[i + 1] - time[i]);
        }
        return speed;
    }

    public static double mad(double[] value) {
        Median median = new Median();
        double mid = median.evaluate(value);
        double[] d = new double[value.length];
        for(int i = 0; i < value.length; ++i) {
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
