package cn.edu.tsinghua.tsquality.storage.impl.iotdb;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.StatsAlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.StatsAlignedRepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.DQType;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Log4j2
@Component("IoTDBStorageEngine")
@ConditionalOnProperty(name = "pre-aggregation.storage-engine", havingValue = "iotdb")
public class IoTDBStorageEngine implements MetadataStorageEngine {
  private final SessionPool sessionPool;

  public IoTDBStorageEngine(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    try {
      for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
        AlignedRepository[] repositories =
            createStatsAlignedRepositoryAndTimeSeriesForPath(entry.getKey());
        saveFileStatsFor(tsFileInfo, entry, repositories);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
    }
  }

  private StatsAlignedRepository[] createStatsAlignedRepositoryAndTimeSeriesForPath(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    return new StatsAlignedRepository[] {
      new StatsAlignedRepositoryImpl(sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.FILE),
      new StatsAlignedRepositoryImpl(sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.CHUNK),
      new StatsAlignedRepositoryImpl(sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.PAGE),
    };
  }

  private void saveFileStatsFor(
      TsFileInfo tsFileInfo, Map.Entry<Path, TsFileStat> entry, AlignedRepository[] repositories)
      throws IoTDBConnectionException, StatementExecutionException {
    saveFileLevelSeriesStats(tsFileInfo, entry.getValue().getFileStat(), repositories[0]);
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry : chunkStats.entrySet()) {
      saveChunkLevelSeriesStats(chunkEntry, repositories[1]);
    }
  }

  private void saveFileLevelSeriesStats(
      TsFileInfo tsFileInfo, IoTDBSeriesStat stat, AlignedRepository fileStatsRepository)
      throws IoTDBConnectionException, StatementExecutionException {
    long timestamp = fileStatsRepository.count();
    List<Object> values = fileLevelSeriesStatsValues(tsFileInfo.getFilePath(), stat);
    fileStatsRepository.insert(timestamp, values);
  }

  private List<Object> fileLevelSeriesStatsValues(String path, IoTDBSeriesStat stat) {
    List<Object> values = new ArrayList<>();
    values.add(path);
    values.addAll(StatsTimeSeriesUtil.getValuesForStat(stat));
    return values;
  }

  private void saveChunkLevelSeriesStats(
      Map.Entry<Long, IoTDBSeriesStat> entry, AlignedRepository chunkStatsRepository)
      throws IoTDBConnectionException, StatementExecutionException {
    long timestamp = chunkStatsRepository.count();
    List<Object> values = chunkLevelSeriesStatsValues(entry.getKey(), entry.getValue());
    chunkStatsRepository.insert(timestamp, values);
  }

  private List<Object> chunkLevelSeriesStatsValues(long offset, IoTDBSeriesStat stat) {
    List<Object> values = new ArrayList<>();
    values.add(offset);
    values.addAll(StatsTimeSeriesUtil.getValuesForStat(stat));
    return values;
  }

  @Override
  public List<IoTDBSeriesStat> selectSeriesStats(String path) {
    return null;
  }

  @Override
  public List<IoTDBSeriesStat> selectDeviceStats(String path) {
    return null;
  }

  @Override
  public List<IoTDBSeriesStat> selectDatabaseStats(String path) {
    return null;
  }

  @Override
  public IoTDBSeriesStat selectAllStats() {
    return null;
  }

  @Override
  public List<Double> getDataQuality(
      List<DQType> dqTypes, String pathStr, List<TimeRange> timeRanges) {
    Path path = new Path(pathStr, true);

    StatsAlignedRepository fileStatsRepository =
        new StatsAlignedRepositoryImpl(
            sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.FILE);
    IoTDBSeriesStat fileStat = fileStatsRepository.selectStats(timeRanges);
    List<TimeRange> fileStatTimeRanges = fileStatsRepository.selectTimeRanges(timeRanges);
    timeRanges = TimeRange.getRemains(timeRanges, fileStatTimeRanges);

    StatsAlignedRepository chunkStatsRepository =
        new StatsAlignedRepositoryImpl(
            sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.CHUNK);
    IoTDBSeriesStat chunkStat = chunkStatsRepository.selectStats(timeRanges);
    List<TimeRange> chunkStatTimeRanges = chunkStatsRepository.selectTimeRanges(timeRanges);
    timeRanges = TimeRange.getRemains(timeRanges, chunkStatTimeRanges);

    IoTDBSeriesStat originalDataStat = getStatFromOriginalData(sessionPool, pathStr, timeRanges);
    return mergeStatsAsDQMetrics(dqTypes, fileStat, chunkStat, originalDataStat);
  }
}
