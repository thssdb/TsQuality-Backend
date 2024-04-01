package cn.edu.tsinghua.tsquality.storage;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.common.time.TimeFormatter;
import cn.edu.tsinghua.tsquality.common.time.TimePeriodType;
import cn.edu.tsinghua.tsquality.common.time.TimeSplitter;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.*;

public interface MetadataStorageEngine {
  List<TsFileInfo> selectAllFiles();

  void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats);

  long selectSeriesCount();

  long selectDevicesCount();

  long selectDatabasesCount();

  List<IoTDBSeriesStat> selectSeriesStats(int pageIndex, int pageSize);

  List<IoTDBSeriesStat> selectDeviceStats(int pageIndex, int pageSize);

  List<IoTDBSeriesStat> selectDatabaseStats(int pageIndex, int pageSize);

  IoTDBSeriesStat selectAllStats();

  default LinkedHashMap<String, List<Double>> getAggregateDataQuality(String timePeriodType, String path, TimeRange timeRange) {
    TimePeriodType splitType = TimePeriodType.fromValue(timePeriodType);
    List<TimeRange> timeRanges = TimeSplitter.split(timeRange, splitType);
    LinkedHashMap<String, List<Double>> result = new LinkedHashMap<>();
    for (TimeRange range : timeRanges) {
      String timePeriodString = TimeFormatter.timestampToTimePeriodString(range.getMin(), splitType);
      List<Double> dataQuality = getDataQuality(Arrays.stream(DQType.values()).toList(), path, List.of(range));
      result.put(timePeriodString, dataQuality);
    }
    return result;
  }

  List<Double> getDataQuality(List<DQType> dqTypes, String path, List<TimeRange> timeRanges);

  default IoTDBSeriesStat getStatFromOriginalData(
      SessionPool sessionPool, String path, List<TimeRange> timeRanges) {
    Repository repository = new RepositoryImpl(sessionPool, path);
    TVList data = repository.select(TimeRange.getTimeFilter(timeRanges), null);
    return new IoTDBSeriesStat(data);
  }

  default List<Double> mergeStatsAsDQMetrics(List<DQType> dqTypes, IoTDBSeriesStat... stats) {
    int len = stats.length;
    if (len == 0) {
      return new ArrayList<>();
    }
    IoTDBSeriesStat firstStat;
    int i;
    for (i = 0; i < len; i++) {
      if (stats[i] != null) {
        break;
      }
    }
    if (i == len) {
      return new ArrayList<>();
    }
    firstStat = stats[i];
    for (int j = i + 1; j < len; j++) {
      firstStat.merge(stats[j]);
    }
    return DataQualityCalculationUtil.statToDQMetrics(firstStat, dqTypes);
  }

  LinkedHashMap<String, Long> getDataSizeDistribution(String timePeriodType, String path, TimeRange timeRange);
}
