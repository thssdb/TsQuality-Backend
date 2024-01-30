package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.impl.IoTDBDataQualityServiceImpl;
import cn.edu.tsinghua.tsquality.service.impl.RDBMSDataQualityServiceImpl;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesDataQualityService;
import java.util.List;
import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class TimeSeriesDataQualityServiceImpl implements TimeSeriesDataQualityService {
  private final RDBMSDataQualityServiceImpl rdbmsDQService;
  private final IoTDBDataQualityServiceImpl iotdbDQService;

  public TimeSeriesDataQualityServiceImpl(
      RDBMSDataQualityServiceImpl rdbmsDQService, IoTDBDataQualityServiceImpl iotdbDQService) {
    this.rdbmsDQService = rdbmsDQService;
    this.iotdbDQService = iotdbDQService;
  }

  @Override
  public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
      String path, DQAggregationType aggregationType, List<TimeRange> timeRanges) {
    TimeSeriesDQAggregationDetailDto rdbmsResult =
        rdbmsDQService.getTimeSeriesDQAggregationDetail(path, aggregationType, timeRanges);
    List<TimeRange> remainingTimeRanges = rdbmsDQService.getRemainingTimeRanges();
    TimeSeriesDQAggregationDetailDto iotdbResult =
        iotdbDQService.getTimeSeriesDQAggregationDetail(path, aggregationType, remainingTimeRanges);
    //        return mergeDQAggregationDetailResults(rdbmsResult, iotdbResult);
    return randomDQAggregationDetail();
  }

  private TimeSeriesDQAggregationDetailDto mergeDQAggregationDetailResults(
      TimeSeriesDQAggregationDetailDto rdbmsResult, TimeSeriesDQAggregationDetailDto iotdbResult) {
    rdbmsResult.merge(iotdbResult);
    return rdbmsResult;
  }

  private TimeSeriesDQAggregationDetailDto randomDQAggregationDetail() {
    TimeSeriesDQAggregationDetailDto result = new TimeSeriesDQAggregationDetailDto();
    Random random = new Random();
    for (int i = 0; i < 7; i++) {
      result
          .getItems()
          .add(
              TimeSeriesDQAggregationDetailDto.Item.builder()
                  .time("2023-12-0" + (i + 1))
                  .dataSize(randomDataSize(random))
                  .completeness(randomDouble(random))
                  .consistency(randomDouble(random))
                  .timeliness(randomDouble(random))
                  .validity(randomDouble(random))
                  .build());
    }
    return result;
  }

  private double randomDouble(Random random) {
    return 0.5 + (1 - 0.5) * random.nextDouble();
  }

  private long randomDataSize(Random random) {
    long min = 1000L, max = 100_000L;
    return min + (long) (Math.abs(random.nextDouble() * (max - min)));
  }
}
