package cn.edu.tsinghua.tsquality.storage.impl.iotdb;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.StatsAlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
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
@ConditionalOnProperty(name = "pre-aggregation.storage.type", havingValue = "iotdb")
public class IoTDBStorageEngine implements MetadataStorageEngine {
  private long storeTime = 0;
  private final SessionPool sessionPool;

  public IoTDBStorageEngine(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public List<TsFileInfo> selectAllFiles() {
    AlignedRepository repository =
        new AlignedRepositoryImpl(
            sessionPool,
            StatsTimeSeriesUtil.FILE_INFO_DEVICE,
            StatsTimeSeriesUtil.FILE_INFO_MEASUREMENTS);
    try {
      List<List<Object>> result = repository.select(null, null);
      List<TsFileInfo> tsFileInfos = new ArrayList<>();
      for (List<Object> row : result) {
        TsFileInfo tsFileInfo = new TsFileInfo();
        tsFileInfo.setFilePath(row.get(0).toString());
        tsFileInfo.setFileVersion((Long) row.get(1));
        tsFileInfos.add(tsFileInfo);
      }
      return tsFileInfos;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error("error selecting all files for pre-aggregating: " + e.getMessage());
      return new ArrayList<>();
    }
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    long start = System.currentTimeMillis();
    try {
      saveTsFileInfo(tsFileInfo);
      for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
        AlignedRepository[] repositories =
            createStatsAlignedRepositoryAndTimeSeriesForPath(entry.getKey());
        saveFileStatsFor(tsFileInfo, entry, repositories);
      }
      storeTime += System.currentTimeMillis() - start;
      System.out.println("IoTDB store time: " + storeTime);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(
          String.format(
              "error saving stats for tsfile %s: %s", tsFileInfo.getFilePath(), e.getMessage()));
    }
  }

  private void saveTsFileInfo(TsFileInfo tsFileInfo)
      throws IoTDBConnectionException, StatementExecutionException {
    AlignedRepository repository =
        new AlignedRepositoryImpl(
            sessionPool,
            StatsTimeSeriesUtil.FILE_INFO_DEVICE,
            StatsTimeSeriesUtil.FILE_INFO_MEASUREMENTS);
    repository.createAlignedTimeSeries(StatsTimeSeriesUtil.FILE_INFO_DATA_TYPES);
    long timestamp = repository.count();
    repository.insert(timestamp, List.of(tsFileInfo.getFilePath(), tsFileInfo.getFileVersion()));
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
    for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry :
        entry.getValue().getChunkStats().entrySet()) {
      saveChunkLevelSeriesStats(chunkEntry, repositories[1]);
      for (Map.Entry<Long, List<IoTDBSeriesStat>> pageEntry :
          entry.getValue().getPageStats().entrySet()) {
        for (int i = 0; i < pageEntry.getValue().size(); i++) {
          savePageLevelSeriesStats(i, pageEntry.getValue().get(i), repositories[2]);
        }
      }
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

  private void savePageLevelSeriesStats(
      int index, IoTDBSeriesStat stat, AlignedRepository pageStatsRepository)
      throws IoTDBConnectionException, StatementExecutionException {
    long timestamp = pageStatsRepository.count();
    List<Object> values = pageLevelSeriesStatsValues(index, stat);
    pageStatsRepository.insert(timestamp, values);
  }

  private List<Object> pageLevelSeriesStatsValues(int index, IoTDBSeriesStat stat) {
    List<Object> values = new ArrayList<>();
    values.add(index);
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
    long start = System.currentTimeMillis();
    Path path = new Path(pathStr, true);

    IoTDBSeriesStat fileStat, chunkStat = null, pageStat = null, originalDataStat = null;

    try {
      StatsAlignedRepository fileStatsRepository =
          new StatsAlignedRepositoryImpl(
              sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.FILE);
      fileStat = fileStatsRepository.selectStats(timeRanges);
      List<TimeRange> fileStatTimeRanges = fileStatsRepository.selectTimeRanges(timeRanges);
      timeRanges = TimeRange.getRemains(timeRanges, fileStatTimeRanges);

      if (!timeRanges.isEmpty()) {
        StatsAlignedRepository chunkStatsRepository =
            new StatsAlignedRepositoryImpl(
                sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.CHUNK);
        chunkStat = chunkStatsRepository.selectStats(timeRanges);
        List<TimeRange> chunkStatTimeRanges = chunkStatsRepository.selectTimeRanges(timeRanges);
        timeRanges = TimeRange.getRemains(timeRanges, chunkStatTimeRanges);
      }

      if (!timeRanges.isEmpty()) {
        StatsAlignedRepository pageStatsRepository =
            new StatsAlignedRepositoryImpl(
                sessionPool, path, StatsAlignedRepositoryImpl.StatLevel.PAGE);
        pageStat = pageStatsRepository.selectStats(timeRanges);
        List<TimeRange> pageStatTimeRanges = pageStatsRepository.selectTimeRanges(timeRanges);
        timeRanges = TimeRange.getRemains(timeRanges, pageStatTimeRanges);
      }

      if (!timeRanges.isEmpty()) {
        originalDataStat = getStatFromOriginalData(sessionPool, pathStr, timeRanges);
      }
      List<Double> result = mergeStatsAsDQMetrics(dqTypes, fileStat, chunkStat, pageStat, originalDataStat);
      System.out.println("IoTDB get data quality time: " + (System.currentTimeMillis() - start) + "ms");
      return result;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(String.format("error get data quality for path %s: %s", pathStr, e.getMessage()));
      throw new RuntimeException(e);
    }
  }
}
