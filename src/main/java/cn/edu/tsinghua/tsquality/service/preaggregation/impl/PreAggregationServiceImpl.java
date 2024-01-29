package cn.edu.tsinghua.tsquality.service.preaggregation.impl;

import cn.edu.tsinghua.tsquality.mappers.database.*;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.preaggregation.PreAggregationUtil;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileInfo;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.PreAggregationService;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class PreAggregationServiceImpl implements PreAggregationService {
  @Value("${pre-aggregation.data-dir:.}")
  public String dataDir;

  private final IoTDBSeriesMapper seriesMapper;
  private final IoTDBFileMapper fileMapper;
  private final IoTDBChunkMapper chunkMapper;
  private final IoTDBPageMapper pageMapper;
  private final IoTDBFileSeriesStatMapper fileSeriesStatMapper;
  private final IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;
  private final IoTDBPageSeriesStatMapper pageSeriesStatMapper;

  PreAggregationServiceImpl(
      IoTDBSeriesMapper seriesMapper,
      IoTDBFileMapper fileMapper,
      IoTDBChunkMapper chunkMapper,
      IoTDBPageMapper pageMapper,
      IoTDBFileSeriesStatMapper fileSeriesStatMapper,
      IoTDBChunkSeriesStatMapper chunkSeriesStatMapper,
      IoTDBPageSeriesStatMapper pageSeriesStatMapper) {
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
  @Scheduled(cron = "${pre-aggregation.scan-cron:0 */10 * * * ?}")
  public void preAggregate() {
    List<TsFileInfo> tsFiles = PreAggregationUtil.getAllTsFiles(dataDir);
    if (tsFiles.isEmpty()) {
      return;
    }
    for (TsFileInfo tsfile : tsFiles) {
      preAggregateTsFile(tsfile);
    }
  }

  public void preAggregateTsFile(TsFileInfo tsfile) {
    String filePath = tsfile.getFilePath();
    Map<Path, TsFileStat> tsFileStats = new HashMap<>();
    Collection<Modification> allModifications = getTsFileModifications(filePath);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      List<Path> paths = getSeriesPathsForAggregation(reader);
      for (Path path : paths) {
        preAggregatePath(path, allModifications, reader, tsFileStats);
      }
      saveTsFileStats(tsfile, paths, tsFileStats);
    } catch (IOException e) {
      log.error(e);
    }
  }

  private Collection<Modification> getTsFileModifications(String filePath) {
    TsFileResource resource = new TsFileResource(new File(filePath));
    return resource.getModFile().getModifications();
  }

  private List<Path> getSeriesPathsForAggregation(TsFileSequenceReader reader) throws IOException {
    return reader.getAllPaths().stream().filter(path -> !path.getMeasurement().isEmpty()).toList();
  }

  private List<Modification> getModificationsForPath(
      Collection<Modification> allModifications, Path path) {
    return allModifications.stream()
        .filter(m -> m.getPath().getFullPath().equals(path.getFullPath()))
        .toList();
  }

  private void preAggregatePath(
      Path path,
      Collection<Modification> allModifications,
      TsFileSequenceReader reader,
      Map<Path, TsFileStat> tsFileStats)
      throws IOException {
    TsFileStat tsFileStat = new TsFileStat(path);
    List<Modification> modifications = getModificationsForPath(allModifications, path);
    Map<Long, IChunkReader> chunkReaders = getChunkReaders(path, reader, modifications);
    preAggregateChunks(tsFileStat, chunkReaders);
    tsFileStats.put(path, tsFileStat);
  }

  private Map<Long, IChunkReader> getChunkReaders(
      Path path, TsFileSequenceReader reader, List<Modification> modifications) throws IOException {
    Map<Long, IChunkReader> chunkReaders = new HashMap<>();
    List<ChunkMetadata> chunkMetadataList = getChunkMetaDataList(path, reader, modifications);
    for (ChunkMetadata metadata : chunkMetadataList) {
      Chunk chunk = reader.readMemChunk(metadata);
      IChunkReader chunkReader = new ChunkReader(chunk, null);
      chunkReaders.put(metadata.getOffsetOfChunkHeader(), chunkReader);
    }
    return chunkReaders;
  }

  private List<ChunkMetadata> getChunkMetaDataList(
      Path path, TsFileSequenceReader reader, List<Modification> modifications) throws IOException {
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path, true);
    if (!modifications.isEmpty()) {
      ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
    }
    return chunkMetadataList;
  }

  private void preAggregateChunks(TsFileStat tsFileStat, Map<Long, IChunkReader> chunkReaders)
      throws IOException {
    for (Map.Entry<Long, IChunkReader> entry : chunkReaders.entrySet()) {
      preAggregateChunk(tsFileStat, entry);
    }
  }

  private void preAggregateChunk(TsFileStat tsFileStat, Map.Entry<Long, IChunkReader> entry)
      throws IOException {
    Long chunkOffset = entry.getKey();
    tsFileStat.startNewChunk(chunkOffset);
    while (entry.getValue().hasNextSatisfiedPage()) {
      preAggregatePage(tsFileStat, entry);
    }
    tsFileStat.endChunk(chunkOffset);
  }

  private void preAggregatePage(TsFileStat tsFileStat, Map.Entry<Long, IChunkReader> entry)
      throws IOException {
    BatchData batchData = entry.getValue().nextPageData();
    if (batchData.getDataType() == TSDataType.VECTOR) {
      return;
    }
    IoTDBSeriesStat stat = new IoTDBSeriesStat(batchData);
    tsFileStat.addPageSeriesStat(entry.getKey(), stat);
  }

  public void saveTsFileStats(
      TsFileInfo tsFileInfo, List<Path> paths, Map<Path, TsFileStat> stats) {
    updateIoTDBSeries(tsFileInfo, paths);
    int fid = updateIoTDBFiles(tsFileInfo);
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveTsFileStatForPath(fid, entry);
    }
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
    List<IoTDBSeries> seriesList = getIoTDBSeriesList(paths, tsFileInfo.getDatabase());
    seriesMapper.insertList(seriesList);
  }

  private List<IoTDBSeries> getIoTDBSeriesList(List<Path> paths, String database) {
    return paths.stream().map(x -> pathToIoTDBSeries(x, database)).toList();
  }

  private IoTDBSeries pathToIoTDBSeries(Path path, String database) {
    return IoTDBSeries.builder()
        .path(path.getFullPath())
        .device(path.getDevice())
        .database(database)
        .build();
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
