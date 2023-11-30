package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import cn.edu.tsinghua.tsquality.common.Util;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.ArrayList;

@Data
public class IoTDBSeriesStat {
    private long cnt;
    private long missCnt = 0;
    private long specialCnt = 0;
    private long lateCnt = 0;
    private long redundancyCnt = 0;
    private long valueCnt = 0;
    private long variationCnt = 0;
    private long speedCnt = 0;
    private long accelerationCnt = 0;
    private long minTimestamp = Long.MAX_VALUE;
    private long maxTimestamp = Long.MIN_VALUE;
    private double[] valueList;
    private double[] timeList;
    // this field is not used in the code,
    // only used to store results returned by SQL queries,
    // it could represent the path of a time series or a device or a database
    private String path;

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
        valueCnt = Util.findOutliers(valueList, k);
        double[] variation = Util.variation(valueList);
        variationCnt = Util.findOutliers(variation, k);
        double[] speed = Util.speed(valueList, timeList);
        speedCnt = Util.findOutliers(speed, k);
        if (speed.length < 2) {
            return;
        }
        double[] speedChange = Util.variation(speed);
        accelerationCnt = Util.findOutliers(speedChange, k);
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
