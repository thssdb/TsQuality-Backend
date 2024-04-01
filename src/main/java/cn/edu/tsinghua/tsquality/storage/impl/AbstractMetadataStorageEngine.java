package cn.edu.tsinghua.tsquality.storage.impl;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.common.time.TimeFormatter;
import cn.edu.tsinghua.tsquality.common.time.TimePeriodType;
import cn.edu.tsinghua.tsquality.common.time.TimeSplitter;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.LinkedHashMap;
import java.util.List;

public abstract class AbstractMetadataStorageEngine implements MetadataStorageEngine {
  protected SessionPool sessionPool;

  @Override
  public LinkedHashMap<String, Long> getDataSizeDistribution(String type, String path, TimeRange timeRange) {
    TimePeriodType timePeriodType = TimePeriodType.fromValue(type);
    List<TimeRange> timeRanges = TimeSplitter.split(timeRange, timePeriodType);
    LinkedHashMap<String, Long> result = new LinkedHashMap<>();

    Repository repository = new RepositoryImpl(sessionPool, path);
    for (TimeRange range : timeRanges) {
      String timePeriod = TimeFormatter.timestampToTimePeriodString(range.getMin(), timePeriodType);
      String timeFilter = range.getTimeFilter();
      long dataSize = repository.count(timeFilter);
      result.put(timePeriod, dataSize);
    }
    return result;
  }
}
