package cn.edu.tsinghua.tsquality.service.preaggregation.impl;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.PreAggregationService;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import lombok.extern.log4j.Log4j2;
import lombok.val;
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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
@Service
public class PreAggregationServiceImpl implements PreAggregationService {
  private static final String DATABASE_REGEX = ".*?/data/datanode/data/sequence/(.*?)/.*";

  @Value("${pre-aggregation.data-dir:.}")
  private String dataDir;

  private final MetadataStorageEngine storageEngine;

  public PreAggregationServiceImpl(MetadataStorageEngine storageEngine) {
    this.storageEngine = storageEngine;
  }

  @Override
  @Scheduled(cron = "${pre-aggregation.scan-cron:0 */10 * * * ?}")
  public void preAggregate() {
    List<TsFileInfo> allTsFiles = getAllTsFiles();
    if (allTsFiles.isEmpty()) {
      return;
    }
    List<TsFileInfo> preAggregatedTsFiles = storageEngine.selectAllFiles();
    List<TsFileInfo> targetTsFiles = new ArrayList<>();

    for (TsFileInfo tsfile : allTsFiles) {
      if (preAggregatedTsFiles.stream().noneMatch(f ->
        f.getFilePath().equals(tsfile.getFilePath()) && f.getFileVersion() == tsfile.getFileVersion()
      )) {
        targetTsFiles.add(tsfile);
      }
    }

    for (TsFileInfo tsfile : targetTsFiles) {
      preAggregateTsFile(tsfile);
    }
  }

  private List<TsFileInfo> getAllTsFiles() {
    return getTsFilesUnderDirectory(new File(dataDir));
  }

  private void preAggregateTsFile(TsFileInfo tsfile) {
    log.info("Pre-aggregating tsfile: {}", tsfile.getFilePath());
    String filePath = tsfile.getFilePath();
    Map<Path, TsFileStat> tsFileStats = new HashMap<>();
    Collection<Modification> allModifications = getTsFileModifications(filePath);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      List<Path> paths = getSeriesPathsForAggregation(reader);
      for (Path path : paths) {
        preAggregatePath(path, allModifications, reader, tsFileStats);
      }
      setStatsVersion(tsfile.getFileVersion(), tsFileStats);
      storageEngine.saveTsFileStats(tsfile, tsFileStats);
    } catch (IOException e) {
      log.error(e);
    }
  }

  private void setStatsVersion(long version, Map<Path, TsFileStat> tsFileStats) {
    for (val entry : tsFileStats.entrySet()) {
      entry.getValue().getFileStat().setVersion(version);
      for (val chunkEntry : entry.getValue().getChunkStats().entrySet()) {
        chunkEntry.getValue().setVersion(version);
      }
      for (val pageEntry : entry.getValue().getPageStats().entrySet()) {
        for (IoTDBSeriesStat stat : pageEntry.getValue()) {
          stat.setVersion(version);
        }
      }
    }
  }

  private List<TsFileInfo> getTsFilesUnderDirectory(File dir) {
    List<TsFileInfo> tsFiles = new ArrayList<>();
    File[] files = dir.listFiles();
    if (files == null) {
      return tsFiles;
    }
    for (File file : files) {
      if (file.isDirectory()) {
        tsFiles.addAll(getTsFilesUnderDirectory(file));
      } else {
        tsFiles.addAll(getTsFileUnderFile(file));
      }
    }
    return tsFiles;
  }

  private List<TsFileInfo> getTsFileUnderFile(File file) {
    String database = getDatabaseForTsFile(file);
    if (isNotTsFile(file) || isNotClosed(file) || database.isEmpty()) {
      return new ArrayList<>();
    }
    return List.of(
        TsFileInfo.builder()
            .filePath(file.getAbsolutePath())
            .fileVersion(getFileVersion(file))
            .database(database)
            .build());
  }

  private boolean isNotTsFile(File file) {
    return !file.getName().endsWith(".tsfile");
  }

  private boolean isNotClosed(File file) {
    String tsFileResourcePath = file.getAbsolutePath() + ".resource";
    File tsFileResource = new File(tsFileResourcePath);
    return !tsFileResource.exists();
  }

  private String getDatabaseForTsFile(File file) {
    String filePath = file.getAbsolutePath();
    Matcher matcher = Pattern.compile(DATABASE_REGEX).matcher(filePath);
    if (!matcher.matches()) {
      return "";
    }
    return matcher.group(1);
  }

  private long getFileVersion(File file) {
    TsFileResource resource = new TsFileResource(file);
    long tsFileSize = resource.getTsFileSize();
    long modFileSize = new File(resource.getModFile().getFilePath()).length();
    return tsFileSize + modFileSize;
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
    TSDataType dataType = getDataTypeForPath(path, reader);
    TsFileStat tsFileStat = new TsFileStat(path, dataType);
    List<Modification> modifications = getModificationsForPath(allModifications, path);
    Map<Long, IChunkReader> chunkReaders = getChunkReaders(path, reader, modifications);
    preAggregateChunks(tsFileStat, chunkReaders);
    tsFileStats.put(path, tsFileStat);
  }

  private TSDataType getDataTypeForPath(Path path, TsFileSequenceReader reader) throws IOException {
    return reader.getFullPathDataTypeMap().get(path.getFullPath());
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
}
