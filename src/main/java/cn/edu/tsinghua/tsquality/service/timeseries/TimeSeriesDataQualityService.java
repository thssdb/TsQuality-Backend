package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.storage.DQType;

import java.util.List;

public interface TimeSeriesDataQualityService {
  TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
      String path, DQAggregationType aggregationType, List<TimeRange> timeRanges);

  List<Double> getTimeSeriesDQMetrics(List<DQType> dqTypes, String path, List<TimeRange> timeRanges);
}
