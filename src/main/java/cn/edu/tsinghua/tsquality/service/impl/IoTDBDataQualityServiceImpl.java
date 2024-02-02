package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesDataQualityService;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class IoTDBDataQualityServiceImpl implements TimeSeriesDataQualityService {
  @Override
  public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
      String path, DQAggregationType aggregationType, List<TimeRange> timeRanges) {
    return new TimeSeriesDQAggregationDetailDto();
  }
}
