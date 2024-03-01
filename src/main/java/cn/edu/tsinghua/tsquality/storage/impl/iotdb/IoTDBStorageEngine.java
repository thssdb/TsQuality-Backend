package cn.edu.tsinghua.tsquality.storage.impl.iotdb;

import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Component;

@Log4j2
@Component("IoTDBStorageEngine")
public class IoTDBStorageEngine implements MetadataStorageEngine {
  private final SessionPool sessionPool;
  private final StatsTimeSeriesUtil statsTimeSeriesUtil;

  public IoTDBStorageEngine(SessionPool sessionPool, StatsTimeSeriesUtil statsTimeSeriesUtil) {
    this.sessionPool = sessionPool;
    this.statsTimeSeriesUtil = statsTimeSeriesUtil;
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

  private AlignedRepository[] createStatsAlignedRepositoryAndTimeSeriesForPath(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    return new AlignedRepository[] {
      createFileStatsAlignedRepositoryAndTimeSeriesForPath(path),
      createChunkStatsAlignedRepositoryAndTimeSeriesForPath(path),
      createPageStatsAlignedRepositoryAndTimeSeriesForPath(path),
    };
  }

  private AlignedRepository createFileStatsAlignedRepositoryAndTimeSeriesForPath(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    String device = statsTimeSeriesUtil.getFileStatsDeviceForPath(path);
    List<String> measurements = statsTimeSeriesUtil.getFileStatsMeasurementsForPath(path);
    List<TSDataType> dataTypes = statsTimeSeriesUtil.getFileStatsDataTypesForPath(path);
    AlignedRepositoryImpl repository = new AlignedRepositoryImpl(sessionPool, device, measurements);
    repository.createAlignedTimeSeries(dataTypes);
    return repository;
  }

  private AlignedRepository createChunkStatsAlignedRepositoryAndTimeSeriesForPath(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    String device = statsTimeSeriesUtil.getChunkStatsDeviceForPath(path);
    List<String> measurements = statsTimeSeriesUtil.getChunkStatsMeasurementsForPath(path);
    List<TSDataType> dataTypes = statsTimeSeriesUtil.getChunkStatsDataTypesForPath(path);
    AlignedRepository repository = new AlignedRepositoryImpl(sessionPool, device, measurements);
    repository.createAlignedTimeSeries(dataTypes);
    return repository;
  }

  private AlignedRepository createPageStatsAlignedRepositoryAndTimeSeriesForPath(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    String device = statsTimeSeriesUtil.getPageStatsDeviceForPath(path);
    List<String> measurements = statsTimeSeriesUtil.getPageStatsMeasurementsForPath(path);
    List<TSDataType> dataTypes = statsTimeSeriesUtil.getPageStatsDataTypesForPath(path);
    AlignedRepository repository = new AlignedRepositoryImpl(sessionPool, device, measurements);
    repository.createAlignedTimeSeries(dataTypes);
    return repository;
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
    values.addAll(statsTimeSeriesUtil.getValuesForStat(stat));
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
    values.addAll(statsTimeSeriesUtil.getValuesForStat(stat));
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
}
