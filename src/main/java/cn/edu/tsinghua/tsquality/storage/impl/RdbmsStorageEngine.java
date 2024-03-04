package cn.edu.tsinghua.tsquality.storage.impl;

import cn.edu.tsinghua.tsquality.mappers.database.*;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Primary
@Component("RdbmsStorageEngine")
public class RdbmsStorageEngine implements MetadataStorageEngine {
  private final DataQualityMapper dataQualityMapper;
  private final IoTDBSeriesMapper seriesMapper;
  private final IoTDBFileMapper fileMapper;
  private final IoTDBChunkMapper chunkMapper;
  private final IoTDBPageMapper pageMapper;
  private final IoTDBFileSeriesStatMapper fileSeriesStatMapper;
  private final IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;
  private final IoTDBPageSeriesStatMapper pageSeriesStatMapper;

  public RdbmsStorageEngine(
      DataQualityMapper dataQualityMapper,
      IoTDBSeriesMapper seriesMapper,
      IoTDBFileMapper fileMapper,
      IoTDBChunkMapper chunkMapper,
      IoTDBPageMapper pageMapper,
      IoTDBFileSeriesStatMapper fileSeriesStatMapper,
      IoTDBChunkSeriesStatMapper chunkSeriesStatMapper,
      IoTDBPageSeriesStatMapper pageSeriesStatMapper) {
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
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    updateIoTDBSeries(tsFileInfo, stats.keySet().stream().toList());
    int fid = updateIoTDBFiles(tsFileInfo);
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveTsFileStatForPath(fid, entry);
    }
  }

  @Override
  public List<IoTDBSeriesStat> selectSeriesStats(String path) {
    return dataQualityMapper.selectSeriesStat(path);
  }

  @Override
  public List<IoTDBSeriesStat> selectDeviceStats(String path) {
    return dataQualityMapper.selectDeviceStat(path);
  }

  @Override
  public List<IoTDBSeriesStat> selectDatabaseStats(String path) {
    return dataQualityMapper.selectDatabaseStat(path);
  }

  @Override
  public IoTDBSeriesStat selectAllStats() {
    return dataQualityMapper.selectAllStat();
  }

  private int updateIoTDBFiles(TsFileInfo tsFileInfo) {
    IoTDBFile file = new IoTDBFile(tsFileInfo.getFilePath(), tsFileInfo.getFileVersion());
    return insertIoTDBFile(file);
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

  private void updateIoTDBSeries(TsFileInfo tsFileInfo, List<Path> paths) {
    List<IoTDBSeries> seriesList = IoTDBSeries.fromPaths(paths, tsFileInfo.getDatabase());
    seriesMapper.insertList(seriesList);
  }

  private void saveTsFileStatForPath(int fid, Map.Entry<Path, TsFileStat> entry) {
    int sid = seriesMapper.selectIdByPath(entry.getKey().getFullPath());
    updateFileSeriesStats(fid, sid, entry.getValue().getFileStat());
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry : chunkStats.entrySet()) {
      saveTsFileStatForChunk(fid, sid, chunkEntry);
    }
  }

  private void updateFileSeriesStats(int fid, int sid, IoTDBSeriesStat stat) {
    fileSeriesStatMapper.insert(fid, sid, stat);
  }

  private void saveTsFileStatForChunk(int fid, int sid, Map.Entry<Long, IoTDBSeriesStat> entry) {
    int cid = updateIoTDBChunks(fid, sid, entry.getKey());
    updateChunkSeriesStats(cid, entry.getValue());
  }

  private int updateIoTDBChunks(int fid, int sid, long offset) {
    IoTDBChunk chunk = new IoTDBChunk(fid, sid, offset);
    chunkMapper.insert(chunk);
    return chunk.getCid();
  }

  private void updateChunkSeriesStats(int cid, IoTDBSeriesStat stat) {
    chunkSeriesStatMapper.insert(cid, stat);
  }
}
