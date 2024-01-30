package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.mappers.database.DataQualityMapper;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesDataQualityService;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.springframework.stereotype.Service;

@Getter
@Service
public class RDBMSDataQualityServiceImpl implements TimeSeriesDataQualityService {

  private List<TimeRange> remainingTimeRanges = new ArrayList<>();

  private final DataQualityMapper dataQualityMapper;

  public RDBMSDataQualityServiceImpl(DataQualityMapper dataQualityMapper) {
    this.dataQualityMapper = dataQualityMapper;
  }

  @Override
  public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
      String path, DQAggregationType aggregationType, List<TimeRange> timeRanges) {
    return switch (aggregationType) {
      case DAY -> getDQAggregationDetailByDay(path, timeRanges);
      case MONTH -> getDQAggregationDetailByMonth(path, timeRanges);
      case YEAR -> getDQAggregationDetailByYear(path, timeRanges);
      default -> throw new IllegalArgumentException("Invalid aggregation type: " + aggregationType);
    };
  }

  private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByDay(
      String path, List<TimeRange> timeRanges) {
    return new TimeSeriesDQAggregationDetailDto();
  }

  private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByMonth(
      String path, List<TimeRange> timeRanges) {
    return new TimeSeriesDQAggregationDetailDto();
  }

  private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByYear(
      String path, List<TimeRange> timeRanges) {
    return new TimeSeriesDQAggregationDetailDto();
  }
}
