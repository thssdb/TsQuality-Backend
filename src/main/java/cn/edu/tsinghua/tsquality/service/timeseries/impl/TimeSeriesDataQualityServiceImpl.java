package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesDataQualityService;
import cn.edu.tsinghua.tsquality.storage.DQType;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;

@Service
public class TimeSeriesDataQualityServiceImpl implements TimeSeriesDataQualityService {
  private final MetadataStorageEngine storageEngine;
  private final SessionPool sessionPool;

  public TimeSeriesDataQualityServiceImpl(MetadataStorageEngine storageEngine, SessionPool sessionPool) {
    this.storageEngine = storageEngine;
    this.sessionPool = sessionPool;
  }

  @Override
  public LinkedHashMap<String, Long> getDataSizeDistribution(String timePeriodType, String path, Long startTimestamp, Long endTimestamp) {
    TimeRange timeRange = getTimeRange(path, startTimestamp, endTimestamp);
    return storageEngine.getDataSizeDistribution(timePeriodType, path, timeRange);
  }

  private TimeRange getTimeRange(String path, Long startTimestamp, Long endTimestamp) {
    long minTimestamp = getMinTimestamp(startTimestamp, path);
    long maxTimestamp = getMaxTimestamp(endTimestamp, path);
    return new TimeRange(minTimestamp, maxTimestamp);
  }

  private long getMinTimestamp(Long startTimestamp, String path) {
    if (startTimestamp != null && startTimestamp > 0) {
      return startTimestamp;
    }
    Repository repository = new RepositoryImpl(sessionPool, path);
    return repository.selectMinTimestamp();
  }

  private long getMaxTimestamp(Long endTimestamp, String path) {
    if (endTimestamp != null && endTimestamp > 0) {
      return endTimestamp;
    }
    Repository repository = new RepositoryImpl(sessionPool, path);
    return repository.selectMaxTimestamp();
  }

  @Override
  public LinkedHashMap<String, List<Double>> getAggregateDQMetrics(String timePeriodType, String path, Long startTimestamp, Long endTimestamp) {
    TimeRange timeRange = getTimeRange(path, startTimestamp, endTimestamp);
    return storageEngine.getAggregateDataQuality(timePeriodType, path, timeRange);
  }


  @Override
  public List<Double> getTimeSeriesDQMetrics(
      List<String> dqTypes, String path, List<TimeRange> timeRanges) {
    return storageEngine.getDataQuality(
        dqTypes.stream().map(DQType::fromValue).toList(), path, timeRanges);
  }
}
