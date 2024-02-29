package cn.edu.tsinghua.tsquality.storage.impl;

import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Component("IoTDBStorageEngine")
public class IoTDBStorageEngine implements MetadataStorageEngine {
  private final SessionPool sessionPool;

  public IoTDBStorageEngine(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    createAlignedTimeSeriesIfNotExists(stats.keySet());
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveFileStatsFor(tsFileInfo, entry);
    }
  }

  private void createAlignedTimeSeriesIfNotExists(Collection<Path> paths) {
    AlignedRepository repository = new AlignedRepositoryImpl(sessionPool, paths.stream().toList());
  }

  private void saveFileStatsFor(TsFileInfo tsFileInfo, Map.Entry<Path, TsFileStat> entry) {
    updateFile(tsFileInfo);
    updateFileSeriesStats(tsFileInfo, entry.getValue().getFileStat());
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry : chunkStats.entrySet()) {
      saveChunkSeriesStats(tsFileInfo, entry.getKey(), chunkEntry);
    }
  }

  private void updateFile(TsFileInfo tsFileInfo) {
  }

  private void updateFileSeriesStats(TsFileInfo tsFileInfo, IoTDBSeriesStat fileStat) {
  }

  private void saveChunkSeriesStats(TsFileInfo tsFileInfo, Path key, Map.Entry<Long, IoTDBSeriesStat> entry) {
    updateChunk(tsFileInfo, key, entry.getKey());
    updateChunkSeriesStats(entry.getValue());
  }

  private void updateChunk(TsFileInfo tsFileInfo, Path key, Long key1) {
  }

  private void updateChunkSeriesStats(IoTDBSeriesStat stat) {
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
