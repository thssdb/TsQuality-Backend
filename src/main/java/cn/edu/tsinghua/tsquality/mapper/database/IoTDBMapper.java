package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileInfo;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class IoTDBMapper {
  private final IoTDBSeriesMapper seriesMapper;
  private final IoTDBFileMapper fileMapper;
  private final IoTDBChunkMapper chunkMapper;
  private final IoTDBPageMapper pageMapper;
  private final IoTDBFileSeriesStatMapper fileSeriesStatMapper;
  private final IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;
  private final IoTDBPageSeriesStatMapper pageSeriesStatMapper;
  private final DataQualityMapper dataQualityMapper;

  public IoTDBMapper(IoTDBSeriesMapper seriesMapper, IoTDBFileMapper fileMapper, IoTDBChunkMapper chunkMapper, IoTDBPageMapper pageMapper, IoTDBFileSeriesStatMapper fileSeriesStatMapper, IoTDBChunkSeriesStatMapper chunkSeriesStatMapper, IoTDBPageSeriesStatMapper pageSeriesStatMapper, DataQualityMapper dataQualityMapper) {
    this.seriesMapper = seriesMapper;
    this.fileMapper = fileMapper;
    this.chunkMapper = chunkMapper;
    this.pageMapper = pageMapper;
    this.fileSeriesStatMapper = fileSeriesStatMapper;
    this.chunkSeriesStatMapper = chunkSeriesStatMapper;
    this.pageSeriesStatMapper = pageSeriesStatMapper;
    this.dataQualityMapper = dataQualityMapper;
  }

  public void createTablesIfNotExists() {
    seriesMapper.createSeriesTable();
    fileMapper.createFileTable();
    chunkMapper.createChunkTable();
    pageMapper.createPageTable();
    fileSeriesStatMapper.createFileSeriesStatTable();
    chunkSeriesStatMapper.createChunkSeriesStatTable();
    pageSeriesStatMapper.createPageSeriesStatTable();
  }

  public void saveTsFileStat(
      TsFileInfo tsFile, List<Path> seriesPaths, Map<Path, TsFileStat> stats) {
    String filePath = tsFile.getFilePath();
    IoTDBFile file = new IoTDBFile(filePath, tsFile.getFileVersion());
    int fid;
    // insert one file into the file table
    int res = fileMapper.insert(file);
    if (res == 1) {
      // insert succeeded
      fid = file.getFid();
    } else {
      // already inserted before
      fid = fileMapper.selectIdByFilePath(file.getFilePath());
    }
    // insert multiple series into the series table
    List<IoTDBSeries> seriesList =
        seriesPaths.stream()
            .map(
                s ->
                    IoTDBSeries.builder()
                        .path(s.getFullPath())
                        .device(s.getDevice())
                        .database(tsFile.getDatabase())
                        .build())
            .toList();
    seriesMapper.insertList(seriesList);
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      Path seriesPath = entry.getKey();
      int sid = seriesMapper.selectIdByPath(seriesPath.getFullPath());
      TsFileStat stat = entry.getValue();
      IoTDBSeriesStat fileStat = stat.getFileStat();
      Map<Long, IoTDBSeriesStat> chunkStats = stat.getChunkStats();
      // insert one file-series stat into the file-series-stat table
      fileSeriesStatMapper.insert(fid, sid, fileStat);
      for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry : chunkStats.entrySet()) {
        IoTDBChunk chunk = new IoTDBChunk(fid, sid, chunkEntry.getKey());
        chunkMapper.insert(chunk); // insert one chunk into the chunk table
        int cid = chunk.getCid();
        // insert one chunk-series stat into the chunk-series-stat table
        chunkSeriesStatMapper.insert(cid, chunkEntry.getValue());
      }
    }
  }

  public List<IoTDBSeriesStat> selectSeriesStat() {
    return dataQualityMapper.selectSeriesStat(null);
  }

  public List<IoTDBSeriesStat> selectDeviceStat(String path) {
    return dataQualityMapper.selectDeviceStat(path);
  }

  public List<IoTDBSeriesStat> selectDatabaseStat(String path) {
    return dataQualityMapper.selectDatabaseStat(path);
  }

  public IoTDBSeriesStat selectAllStat() {
    return dataQualityMapper.selectAllStat();
  }
}
