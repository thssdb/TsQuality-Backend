package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.ArrayList;

@Data
public class IoTDBSeriesStat {
    private int cnt;
    private int missCnt = 0;
    private int specialCnt = 0;
    private int lateCnt = 0;
    private int redundancyCnt = 0;
    private int valueCnt = 0;
    private int variationCnt = 0;
    private int speedCnt = 0;
    private int accelerationCnt = 0;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;
    private double[] valueList;
    private double[] timeList;
    private String path;
    private String device;
    private String database;

    public IoTDBSeriesStat() {}

    public IoTDBSeriesStat(BatchData batchData) {
        if (batchData.isEmpty()) {
            return;
        }
        cnt = batchData.length();
        minTimestamp = batchData.getMinTimestamp();
        maxTimestamp = batchData.getMaxTimestamp();

        boolean isNumericType = true;
        ArrayList<Double> times = new ArrayList<>();
        ArrayList<Double> values = new ArrayList<>();
        while (batchData.hasCurrent()) {
            Double value = null;
            double currentTime = (double) batchData.currentTime();
            switch (batchData.getDataType()) {
                case INT32:
                    value = (double) batchData.getInt();
                    break;
                case INT64:
                    value = (double) batchData.getLong();
                    break;
                case FLOAT:
                    value = (double) batchData.getFloat();
                    break;
                case DOUBLE:
                    value = batchData.getDouble();
                    break;
                default:
                    isNumericType = false;
                    break;
            }
            times.add(currentTime);
            if (isNumericType) {
                if (Double.isFinite(value)) {
                    values.add(value);
                } else {
                    specialCnt++;
                    values.add(Double.NaN);
                }
            }
            batchData.next();
        }
        timeList = Util.toDoubleArray(times);
        timeDetect();
        if (isNumericType) {
            valueList = Util.toDoubleArray(values);
            valueDetect();
        }
    }

    public void merge(IoTDBSeriesStat seriesStat) {
        cnt += seriesStat.cnt;
        missCnt += seriesStat.missCnt;
        specialCnt += seriesStat.specialCnt;
        lateCnt += seriesStat.lateCnt;
        redundancyCnt += seriesStat.redundancyCnt;
        valueCnt += seriesStat.valueCnt;
        variationCnt += seriesStat.variationCnt;
        speedCnt += seriesStat.speedCnt;
        accelerationCnt += seriesStat.accelerationCnt;
        minTimestamp = Math.min(minTimestamp, seriesStat.minTimestamp);
        maxTimestamp = Math.max(maxTimestamp, seriesStat.maxTimestamp);
    }

    private void valueDetect() {
        if (valueList.length < 2) {
            return;
        }
        double k = 3;
        valueCnt = findOutliers(valueList, k);
        double[] variation = Util.variation(valueList);
        variationCnt = findOutliers(variation, k);
        double[] speed = Util.speed(valueList, timeList);
        speedCnt = findOutliers(speed, k);
        if (speed.length < 2) {
            return;
        }
        double[] speedChange = Util.variation(speed);
        accelerationCnt = findOutliers(speedChange, k);
    }

    private int findOutliers(double[] value, double k) {
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

    public void timeDetect() {
        double[] interval = Util.variation(timeList);
        Median median = new Median();
        double base = median.evaluate(interval);
        ArrayList<Double> window = new ArrayList<>();

        int i;
        for (i = 0; i < Math.min(timeList.length, 10); ++i) {
            window.add(timeList[i]);
        }

        while (window.size() > 1) {
            double times = (window.get(1) - window.get(0)) / base;
            if (times <= 0.5D) {
                window.remove(1);
                ++redundancyCnt;
            } else if (times >= 2.0D && times <= 9.0D) {
                int temp = 0;

                for (int j = 2; j < window.size(); ++j) {
                    double times2 = (window.get(j) - window.get(j - 1)) / base;
                    if (times2 >= 2.0D) {
                        break;
                    }

                    if (times2 <= 0.5D) {
                        ++temp;
                        window.remove(j);
                        --j;
                        if (temp == (int) Math.round(times - 1.0D)) {
                            break;
                        }
                    }
                }
                lateCnt += temp;
                missCnt = (int) ((long) missCnt + (Math.round(times - 1.0D) - (long) temp));
            }
            window.remove(0);
            while (window.size() < 10 && i < timeList.length) {
                window.add(timeList[i]);
                ++i;
            }
        }
    }
}
