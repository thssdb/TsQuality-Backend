package cn.edu.tsinghua.tsquality.model.entity;

import cn.edu.tsinghua.tsquality.common.Util;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import lombok.Data;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.ArrayList;
import java.util.List;

@Data
public class IoTDBSeriesStat {
  private long minTimestamp = Long.MAX_VALUE;
  private long maxTimestamp = Long.MIN_VALUE;
  private long count;
  private long missCount = 0;
  private long specialCount = 0;
  private long lateCount = 0;
  private long redundancyCount = 0;
  private long valueCount = 0;
  private long variationCount = 0;
  private long speedCount = 0;
  private long accelerationCount = 0;
  private double[] valueList;
  private double[] timeList;
  // this field is not used in the code,
  // only used to store results returned by SQL queries,
  // it could represent the path of a time series or a device or a database
  private String path;

  public IoTDBSeriesStat() {}

  public IoTDBSeriesStat(TVList tvList) {
    int size = tvList.size();
    if (size == 0) {
      return;
    }

    count = size;
    minTimestamp = tvList.getTimestamp(0);
    maxTimestamp = tvList.getTimestamp(size - 1);

    boolean isNumericType = true;
    List<Double> times = new ArrayList<>();
    List<Double> values = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Double value = null;
      double currentTime = (double) tvList.getTimestamp(i);
      switch (tvList.getDataType()) {
        case INT32:
          value = (double) tvList.getIntPair(i).getInt();
          break;
        case INT64:
          value = (double) tvList.getLongPair(i).getLong();
          break;
        case FLOAT:
          value = (double) tvList.getFloatPair(i).getFloat();
          break;
        case DOUBLE:
          value = (double) tvList.getDoublePair(i).getDouble();
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
          specialCount++;
          values.add(Double.NaN);
        }
      }
    }
    timeList = Util.toDoubleArray(times);
    timeDetect();
    if (isNumericType) {
      valueList = Util.toDoubleArray(values);
      valueDetect();
    }
  }

  public IoTDBSeriesStat(BatchData batchData) {
    if (batchData.isEmpty()) {
      return;
    }
    count = batchData.length();
    minTimestamp = batchData.getMinTimestamp();
    maxTimestamp = batchData.getMaxTimestamp();

    boolean isNumericType = true;
    List<Double> times = new ArrayList<>();
    List<Double> values = new ArrayList<>();
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
          specialCount++;
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

  public IoTDBSeriesStat merge(IoTDBSeriesStat seriesStat) {
    count += seriesStat.count;
    missCount += seriesStat.missCount;
    specialCount += seriesStat.specialCount;
    lateCount += seriesStat.lateCount;
    redundancyCount += seriesStat.redundancyCount;
    valueCount += seriesStat.valueCount;
    variationCount += seriesStat.variationCount;
    speedCount += seriesStat.speedCount;
    accelerationCount += seriesStat.accelerationCount;
    minTimestamp = Math.min(minTimestamp, seriesStat.minTimestamp);
    maxTimestamp = Math.max(maxTimestamp, seriesStat.maxTimestamp);
    return this;
  }

  private void valueDetect() {
    if (valueList.length < 2) {
      return;
    }
    double k = 3;
    valueCount = Util.findOutliers(valueList, k);
    double[] variation = Util.variation(valueList);
    variationCount = Util.findOutliers(variation, k);
    double[] speed = Util.speed(valueList, timeList);
    speedCount = Util.findOutliers(speed, k);
    if (speed.length < 2) {
      return;
    }
    double[] speedChange = Util.variation(speed);
    accelerationCount = Util.findOutliers(speedChange, k);
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
        ++redundancyCount;
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
        lateCount += temp;
        missCount = (int) ((long) missCount + (Math.round(times - 1.0D) - (long) temp));
      }
      window.remove(0);
      while (window.size() < 10 && i < timeList.length) {
        window.add(timeList[i]);
        ++i;
      }
    }
  }
}
