package cn.edu.tsinghua.tsquality.model.entity;

import cn.edu.tsinghua.tsquality.common.Util;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.EmptyTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.storage.impl.iotdb.StatsTimeSeriesUtil;
import lombok.Data;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
public class IoTDBSeriesStat {
  private long version = 0;
  private long minTime = Long.MAX_VALUE;
  private long maxTime = Long.MIN_VALUE;
  private double minValue = Long.MAX_VALUE;
  private double maxValue = Long.MIN_VALUE;
  private long count = 0;
  private long missCount = 0;
  private long specialCount = 0;
  private long lateCount = 0;
  private long redundancyCount = 0;
  private long valueCount = 0;
  private long variationCount = 0;
  private long speedCount = 0;
  private long accelerationCount = 0;
  private double[] valueList = new double[] {};
  private double[] timeList = new double[] {};
  // this field is not used in the code,
  // only used to store results returned by SQL queries,
  // it could represent the path of a time series or a device or a database
  private String path;

  public IoTDBSeriesStat() {}

  public IoTDBSeriesStat(Row row) {
    minTime = Long.parseLong(row.getAs("minTime"));
    maxTime = Long.parseLong(row.getAs("maxTime"));
    count = ((Double) row.getAs("count")).longValue();
    missCount = ((Double) row.getAs("missCount")).longValue();
    specialCount = ((Double) row.getAs("specialCount")).longValue();
    lateCount = ((Double) row.getAs("lateCount")).longValue();
    redundancyCount = ((Double) row.getAs("redundancyCount")).longValue();
    valueCount = ((Double) row.getAs("valueCount")).longValue();
    variationCount = ((Double) row.getAs("variationCount")).longValue();
    speedCount = ((Double) row.getAs("speedCount")).longValue();
    accelerationCount = ((Double) row.getAs("accelerationCount")).longValue();
  }

  public IoTDBSeriesStat(SessionDataSetWrapper wrapper)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet.DataIterator iterator = wrapper.iterator();
    if (!iterator.next()) {
      return;
    }
    minTime = iterator.getLong(StatsTimeSeriesUtil.MIN_TIME);
    maxTime = iterator.getLong(StatsTimeSeriesUtil.MAX_TIME);
    count = (long) iterator.getDouble(StatsTimeSeriesUtil.COUNT);
    missCount = (long) iterator.getDouble(StatsTimeSeriesUtil.MISS_COUNT);
    specialCount = (long) iterator.getDouble(StatsTimeSeriesUtil.SPECIAL_COUNT);
    lateCount = (long) iterator.getDouble(StatsTimeSeriesUtil.LATE_COUNT);
    redundancyCount = (long) iterator.getDouble(StatsTimeSeriesUtil.REDUNDANT_COUNT);
    valueCount = (long) iterator.getDouble(StatsTimeSeriesUtil.VALUE_COUNT);
    variationCount = (long) iterator.getDouble(StatsTimeSeriesUtil.VARIATION_COUNT);
    speedCount = (long) iterator.getDouble(StatsTimeSeriesUtil.SPEED_COUNT);
    accelerationCount = (long) iterator.getDouble(StatsTimeSeriesUtil.ACCELERATION_COUNT);
  }

  public IoTDBSeriesStat(TVList tvList) {
    if (tvList instanceof EmptyTVList) {
      return;
    }
    int size = tvList.size();
    if (size == 0) {
      return;
    }

    count = size;
    minTime = tvList.getTimestamp(0);
    maxTime = tvList.getTimestamp(size - 1);

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
    minTime = batchData.getMinTimestamp();
    maxTime = batchData.getMaxTimestamp();

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
    if (seriesStat == null) {
      return this;
    }
    minTime = Math.min(minTime, seriesStat.minTime);
    maxTime = Math.max(maxTime, seriesStat.maxTime);
    minValue = Math.min(minValue, seriesStat.minValue);
    maxValue = Math.max(maxValue, seriesStat.maxValue);
    count += seriesStat.count;
    missCount += seriesStat.missCount;
    specialCount += seriesStat.specialCount;
    lateCount += seriesStat.lateCount;
    redundancyCount += seriesStat.redundancyCount;
    valueCount += seriesStat.valueCount;
    variationCount += seriesStat.variationCount;
    speedCount += seriesStat.speedCount;
    accelerationCount += seriesStat.accelerationCount;
    return this;
  }

  private void valueDetect() {
    if (valueList.length < 2) {
      return;
    }
    minValue = Arrays.stream(valueList).min().getAsDouble();
    maxValue = Arrays.stream(valueList).max().getAsDouble();
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
        missCount = (int) (missCount + (Math.round(times - 1.0D) - (long) temp));
      }
      window.remove(0);
      while (window.size() < 10 && i < timeList.length) {
        window.add(timeList[i]);
        ++i;
      }
    }
  }
}
