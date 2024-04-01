package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;

import java.util.LinkedHashMap;
import java.util.List;

public interface TimeSeriesDataQualityService {
  LinkedHashMap<String, Long> getDataSizeDistribution(String timePeriodType, String path, Long startTimestamp, Long endTimestamp);

  LinkedHashMap<String, List<Double>> getAggregateDQMetrics(
      String timePeriodType, String path, Long startTimestamp, Long endTimestamp);

  List<Double> getTimeSeriesDQMetrics(
      List<String> dqTypes, String path, List<TimeRange> timeRanges);
}
