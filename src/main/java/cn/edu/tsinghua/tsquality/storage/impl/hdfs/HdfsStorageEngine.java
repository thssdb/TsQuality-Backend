package cn.edu.tsinghua.tsquality.storage.impl.hdfs;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.DQType;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities.ChunkLevelStat;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities.FileLevelStat;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities.MetadataStat;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities.PageLevelStat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component("hdfsStorageEngine")
@ConditionalOnProperty(name = "pre-aggregation.storage-engine", havingValue = "hdfs")
public class HdfsStorageEngine implements MetadataStorageEngine {
  @Value("${hdfs.partition:1}")
  int partition;

  private final Configuration conf;
  private final SparkSession spark;
  private final SessionPool sessionPool;

  public HdfsStorageEngine(Configuration config, SparkSession spark, SessionPool sessionPool)
      throws IOException {
    this.conf = config;
    this.spark = spark;
    this.sessionPool = sessionPool;
    createMetadataDirsIfNotExists();
  }

  private void createMetadataDirsIfNotExists() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.metadataDirname));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.fileSeriesStatsDirName));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.chunkSeriesStatsDirName));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.pageSeriesStatsDirName));
  }

  @Override
  public List<TsFileInfo> selectAllFiles() {
    return null;
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveTsFileStatsForPath(tsFileInfo, entry);
    }
  }

  private void saveTsFileStatsForPath(TsFileInfo tsFileInfo, Map.Entry<Path, TsFileStat> entry) {
    saveFileLevelStats(tsFileInfo, entry);
    saveChunkLevelStats(tsFileInfo, entry);
    savePageLevelStats(tsFileInfo, entry);
  }

  private void saveFileLevelStats(TsFileInfo info, Map.Entry<Path, TsFileStat> entry) {
    FileLevelStat fileLevelStat =
        new FileLevelStat(entry.getKey().getFullPath(), info, entry.getValue().getFileStat());
    Dataset<FileLevelStat> statDataset =
        spark.createDataset(List.of(fileLevelStat), Encoders.bean(FileLevelStat.class));
    statDataset
        .repartition(partition)
        .write()
        .option("header", true)
        .option("compression", "gzip")
        .mode(SaveMode.Append)
        .csv(HdfsStorageConstants.fileSeriesStatsDirName);
  }

  private void saveChunkLevelStats(TsFileInfo info, Map.Entry<Path, TsFileStat> entry) {
    String path = entry.getKey().getFullPath();
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    List<ChunkLevelStat> chunkLevelStats = new ArrayList<>();
    List<Long> offsets = chunkStats.keySet().stream().sorted().toList();
    for (Long offset : offsets) {
      ChunkLevelStat stat = new ChunkLevelStat(path, info, chunkStats.get(offset), offset);
      chunkLevelStats.add(stat);
    }
    Dataset<ChunkLevelStat> statDataset =
        spark.createDataset(chunkLevelStats, Encoders.bean(ChunkLevelStat.class));
    statDataset
        .repartition(partition)
        .write()
        .option("header", true)
        .option("compression", "gzip")
        .mode(SaveMode.Append)
        .csv(HdfsStorageConstants.chunkSeriesStatsDirName);
  }

  private void savePageLevelStats(TsFileInfo tsFileInfo, Map.Entry<Path, TsFileStat> entry) {
    String path = entry.getKey().getFullPath();
    Map<Long, List<IoTDBSeriesStat>> pageStats = entry.getValue().getPageStats();
    List<Long> offsets = pageStats.keySet().stream().sorted().toList();
    List<PageLevelStat> pageLevelStats = new ArrayList<>();
    for (int i = 0; i < offsets.size(); i++) {
      Long offset = offsets.get(i);
      List<IoTDBSeriesStat> stats = pageStats.get(offset);
      for (int j = 0; j < stats.size(); j++) {
        PageLevelStat stat =
            new PageLevelStat(path, tsFileInfo, stats.get(j), String.format("%d-%d", i + 1, j + 1));
        pageLevelStats.add(stat);
      }
    }
    Dataset<PageLevelStat> statDataset =
        spark.createDataset(pageLevelStats, Encoders.bean(PageLevelStat.class));
    statDataset
        .repartition(partition)
        .write()
        .option("header", true)
        .option("compression", "gzip")
        .mode(SaveMode.Append)
        .csv(HdfsStorageConstants.pageSeriesStatsDirName);
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
      List<DQType> dqTypes, String path, List<TimeRange> timeRanges) {
    Dataset<Row> fileDataset =
        readDatasetFromCsv(HdfsStorageConstants.fileSeriesStatsDirName, path, timeRanges);
    IoTDBSeriesStat fileLevelStat = getStatFromDataset(fileDataset);
    List<TimeRange> fileLevelTimeRanges = getTimeRangesFromDataset(fileDataset);
    timeRanges = TimeRange.getRemains(timeRanges, fileLevelTimeRanges);

    Dataset<Row> chunkDataset =
        readDatasetFromCsv(HdfsStorageConstants.chunkSeriesStatsDirName, path, timeRanges);
    IoTDBSeriesStat chunkLevelStat = getStatFromDataset(chunkDataset);
    List<TimeRange> chunkLevelTimeRanges = getTimeRangesFromDataset(chunkDataset);
    timeRanges = TimeRange.getRemains(timeRanges, chunkLevelTimeRanges);

    Dataset<Row> pageDataset =
        readDatasetFromCsv(HdfsStorageConstants.pageSeriesStatsDirName, path, timeRanges);
    IoTDBSeriesStat pageLevelStat = getStatFromDataset(pageDataset);
    List<TimeRange> pageLevelTimeRanges = getTimeRangesFromDataset(pageDataset);
    timeRanges = TimeRange.getRemains(timeRanges, pageLevelTimeRanges);

    IoTDBSeriesStat originalDataStat = getStatFromOriginalData(sessionPool, path, timeRanges);
    return mergeStatsAsDQMetrics(
        dqTypes, fileLevelStat, chunkLevelStat, pageLevelStat, originalDataStat);
  }

  private Dataset<Row> readDatasetFromCsv(
      String filePath, String seriesPath, List<TimeRange> timeRanges) {
    Dataset<Row> dataset = spark.read().csv(filePath);
    return dataset
        .filter(col("path").equalTo(seriesPath))
        .filter(col("filePath").equalTo(filePath))
        .filter(getTimeFilter(timeRanges));
  }

  private IoTDBSeriesStat getStatFromDataset(Dataset<Row> dataset) {
    Row row = dataset.selectExpr(MetadataStat.statSumColumns()).first();
    return new IoTDBSeriesStat(row);
  }

  private List<TimeRange> getTimeRangesFromDataset(Dataset<Row> dataset) {
    return dataset
        .selectExpr(MetadataStat.timeColumns())
        .map(
            (MapFunction<Row, TimeRange>)
                row -> {
                  long minTime = row.getAs("minTime");
                  long maxTime = row.getAs("maxTime");
                  return new TimeRange(minTime, maxTime);
                },
            Encoders.bean(TimeRange.class))
        .collectAsList();
  }

  private Column getTimeFilter(List<TimeRange> timeRanges) {
    Column filter = null;
    if (timeRanges == null) {
      return null;
    }

    for (TimeRange timeRange : timeRanges) {
      Column minTimeCondition =
          timeRange.isLeftClose()
              ? col("minTime").geq(timeRange.getMin())
              : col("minTime").gt(timeRange.getMin());
      Column maxTimeCondition =
          timeRange.isRightClose()
              ? col("maxTime").leq(timeRange.getMax())
              : col("maxTime").lt(timeRange.getMax());
      Column timeRangeCondition = minTimeCondition.and(maxTimeCondition);
      filter = filter == null ? timeRangeCondition : filter.or(timeRangeCondition);
    }
    return filter;
  }
}
