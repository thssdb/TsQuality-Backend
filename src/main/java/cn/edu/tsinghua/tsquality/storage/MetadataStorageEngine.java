package cn.edu.tsinghua.tsquality.storage;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;

public interface MetadataStorageEngine {
  void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats);

  List<IoTDBSeriesStat> selectSeriesStats(String path);

  List<IoTDBSeriesStat> selectDeviceStats(String path);

  List<IoTDBSeriesStat> selectDatabaseStats(String path);

  IoTDBSeriesStat selectAllStats();

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
    IoTDBSeriesStat firstStat = stats[0];
    for (int i = 1; i < len; i++) {
      firstStat.merge(stats[i]);
    }
    return DataQualityCalculationUtil.statToDQMetrics(firstStat, dqTypes);
  }
}
