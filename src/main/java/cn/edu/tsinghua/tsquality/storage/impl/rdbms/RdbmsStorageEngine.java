package cn.edu.tsinghua.tsquality.storage.impl.rdbms;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.mappers.database.*;
import cn.edu.tsinghua.tsquality.model.entity.*;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.DQType;
import cn.edu.tsinghua.tsquality.storage.impl.AbstractMetadataStorageEngine;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component("RdbmsStorageEngine")
@ConditionalOnProperty(
    name = "pre-aggregation.storage.type",
    havingValue = "rdbms",
    matchIfMissing = true)
public class RdbmsStorageEngine extends AbstractMetadataStorageEngine {
  private final DataQualityMapper dataQualityMapper;
  private final IoTDBSeriesMapper seriesMapper;
  private final IoTDBFileMapper fileMapper;
  private final IoTDBChunkMapper chunkMapper;
  private final IoTDBPageMapper pageMapper;
  private final IoTDBFileSeriesStatMapper fileSeriesStatMapper;
  private final IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;
  private final IoTDBPageSeriesStatMapper pageSeriesStatMapper;

  public RdbmsStorageEngine(
      SessionPool sessionPool,
      DataQualityMapper dataQualityMapper,
      IoTDBSeriesMapper seriesMapper,
      IoTDBFileMapper fileMapper,
      IoTDBChunkMapper chunkMapper,
      IoTDBPageMapper pageMapper,
      IoTDBFileSeriesStatMapper fileSeriesStatMapper,
      IoTDBChunkSeriesStatMapper chunkSeriesStatMapper,
      IoTDBPageSeriesStatMapper pageSeriesStatMapper) {
    this.sessionPool = sessionPool;
    this.dataQualityMapper = dataQualityMapper;
    this.seriesMapper = seriesMapper;
    this.fileMapper = fileMapper;
    this.chunkMapper = chunkMapper;
    this.pageMapper = pageMapper;
    this.fileSeriesStatMapper = fileSeriesStatMapper;
    this.chunkSeriesStatMapper = chunkSeriesStatMapper;
    this.pageSeriesStatMapper = pageSeriesStatMapper;
    createTablesIfNotExists();
  }

  private void createTablesIfNotExists() {
    seriesMapper.createSeriesTable();
    fileMapper.createFileTable();
    chunkMapper.createChunkTable();
    pageMapper.createPageTable();
    fileSeriesStatMapper.createFileSeriesStatTable();
    chunkSeriesStatMapper.createChunkSeriesStatTable();
    pageSeriesStatMapper.createPageSeriesStatTable();
  }

  @Override
  public List<TsFileInfo> selectAllFiles() {
    return fileMapper.select().stream().map(TsFileInfo::new).toList();
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    long start = System.currentTimeMillis();
    updateIoTDBSeries(tsFileInfo, stats.keySet().stream().toList());
    int fid = updateIoTDBFiles(tsFileInfo);
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveTsFileStatForPath(fid, entry);
    }
  }

  private void updateIoTDBSeries(TsFileInfo tsFileInfo, List<Path> paths) {
    List<IoTDBSeries> seriesList = IoTDBSeries.fromPaths(paths, tsFileInfo.getDatabase());
    seriesMapper.insertList(seriesList);
  }

  private int updateIoTDBFiles(TsFileInfo tsFileInfo) {
    IoTDBFile file = new IoTDBFile(tsFileInfo.getFilePath(), tsFileInfo.getFileVersion());
    return insertIoTDBFile(file);
  }

  private void saveTsFileStatForPath(int fid, Map.Entry<Path, TsFileStat> entry) {
    Map<Long, Integer> chunkOffsetToCid = new HashMap<>();

    int sid = seriesMapper.selectIdByPath(entry.getKey().getFullPath());
    updateFileSeriesStats(fid, sid, entry.getValue().getFileStat());
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry : chunkStats.entrySet()) {
      saveTsFileStatForChunk(fid, sid, chunkEntry, chunkOffsetToCid);
    }
    Map<Long, List<IoTDBSeriesStat>> pageStats = entry.getValue().getPageStats();
    for (Map.Entry<Long, List<IoTDBSeriesStat>> pageEntry : pageStats.entrySet()) {
      for (IoTDBSeriesStat stat : pageEntry.getValue()) {
        int cid = chunkOffsetToCid.get(pageEntry.getKey());
        int pid = updateIoTDBPages(cid, sid, pageEntry.getKey() + 1);
        updatePageSeriesStats(pid, sid, stat);
      }
    }
  }

  private int insertIoTDBFile(IoTDBFile file) {
    int res = fileMapper.insert(file);
    if (res == 1) {
      // insert succeed
      return file.getFid();
    }
    // already inserted before
    return fileMapper.selectIdByFilePath(file.getFilePath());
  }

  private void updateFileSeriesStats(int fid, int sid, IoTDBSeriesStat stat) {
    fileSeriesStatMapper.insert(fid, sid, stat);
  }

  private void saveTsFileStatForChunk(
      int fid,
      int sid,
      Map.Entry<Long, IoTDBSeriesStat> entry,
      Map<Long, Integer> chunkOffsetToCid) {
    int cid = updateIoTDBChunks(fid, sid, entry.getKey());
    chunkOffsetToCid.put(entry.getKey(), cid);
    updateChunkSeriesStats(cid, sid, entry.getValue());
  }

  private int updateIoTDBChunks(int fid, int sid, long offset) {
    IoTDBChunk chunk = new IoTDBChunk(fid, sid, offset);
    chunkMapper.insert(chunk);
    return chunk.getCid();
  }

  private void updateChunkSeriesStats(int cid, int sid, IoTDBSeriesStat stat) {
    chunkSeriesStatMapper.insert(cid, sid, stat);
  }

  private int updateIoTDBPages(int cid, int sid, long index) {
    IoTDBPage page = new IoTDBPage(cid, sid, index);
    pageMapper.insert(page);
    return page.getPid();
  }

  private void updatePageSeriesStats(int pid, int sid, IoTDBSeriesStat stat) {
    pageSeriesStatMapper.insert(pid, sid, stat);
  }

  @Override
  public long selectSeriesCount() {
    return dataQualityMapper.selectSeriesCount();
  }

  @Override
  public long selectDevicesCount() {
    return dataQualityMapper.selectDevicesCount();
  }

  @Override
  public long selectDatabasesCount() {
    return dataQualityMapper.selectDatabasesCount();
  }

  @Override
  public List<IoTDBSeriesStat> selectSeriesStats(int pageIndex, int pageSize) {
    return dataQualityMapper.selectSeriesStat(pageSize, (pageIndex - 1) * pageSize);
  }

  @Override
  public List<IoTDBSeriesStat> selectDeviceStats(int pageIndex, int pageSize) {
    return dataQualityMapper.selectDeviceStat(pageSize, (pageIndex - 1) * pageSize);
  }

  @Override
  public List<IoTDBSeriesStat> selectDatabaseStats(int pageIndex, int pageSize) {
    return dataQualityMapper.selectDatabaseStat(pageSize, (pageIndex - 1) * pageSize);
  }

  @Override
  public IoTDBSeriesStat selectAllStats() {
    return dataQualityMapper.selectAllStat();
  }

  @Override
  public List<Double> getDataQuality(
      List<DQType> dqTypes, String path, List<TimeRange> timeRanges) {
    IoTDBSeriesStat fileStat, chunkStat = null, originalDataStat = null;

    fileStat = fileSeriesStatMapper.selectStats(path, timeRanges);
    List<TimeRange> fileStatTimeRanges = fileSeriesStatMapper.selectTimeRanges(path, timeRanges);
    timeRanges = TimeRange.getRemains(timeRanges, fileStatTimeRanges);

    if (!timeRanges.isEmpty()) {
      chunkStat = chunkSeriesStatMapper.selectStats(path, timeRanges);
      List<TimeRange> chunkStatTimeRanges =
          chunkSeriesStatMapper.selectTimeRanges(path, timeRanges);
      timeRanges = TimeRange.getRemains(timeRanges, chunkStatTimeRanges);
    }

    if (!timeRanges.isEmpty()) {
      originalDataStat = getStatFromOriginalData(sessionPool, path, timeRanges);
    }
    return mergeStatsAsDQMetrics(dqTypes, fileStat, chunkStat, originalDataStat);
  }
}
