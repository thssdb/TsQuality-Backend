package cn.edu.tsinghua.tsquality.storage.impl.hdfs;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.DQType;
import cn.edu.tsinghua.tsquality.storage.impl.AbstractMetadataStorageEngine;
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
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component("hdfsStorageEngine")
@ConditionalOnProperty(name = "pre-aggregation.storage.type", havingValue = "hdfs")
public class HdfsStorageEngine extends AbstractMetadataStorageEngine {
  @Value("${hdfs.partition:1}")
  int partition;

  @Value("${pre-aggregation.storage.hdfs.compression:none}")
  String compression;

  private long storeTime = 0;

  private final Configuration conf;
  private final SparkSession spark;

  public HdfsStorageEngine(Configuration config, SparkSession spark, SessionPool sessionPool)
      throws IOException {
    this.conf = config;
    this.spark = spark;
    this.sessionPool = sessionPool;
    createMetadataDirsIfNotExists();
  }

  private void createMetadataDirsIfNotExists() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.METADATA_DIRNAME));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.FILES_DIRNAME));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.FILE_SERIES_STATS_DIRNAME));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.CHUNK_SERIES_STATS_DIRNAME));
    fs.mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.PAGE_SERIES_STATS_DIRNAME));
  }

  @Override
  public List<TsFileInfo> selectAllFiles() {
    StructType schema = new StructType().add("filePath", "string").add("fileVersion", "long");
    Dataset<TsFileInfo> dataset =
        spark
            .read()
            .schema(schema)
            .option("header", true)
            .csv(HdfsStorageConstants.FILES_DIRNAME)
            .map(
                (MapFunction<Row, TsFileInfo>)
                    row -> {
                      String filePath = row.getAs("filePath");
                      long fileVersion = row.getAs("fileVersion");
                      return new TsFileInfo(filePath, fileVersion);
                    },
                Encoders.bean(TsFileInfo.class));
    return dataset.collectAsList();
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    long start = System.currentTimeMillis();
    saveTsFileInfo(tsFileInfo);
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      saveTsFileStatsForPath(tsFileInfo, entry);
    }
    storeTime += System.currentTimeMillis() - start;
    System.out.println("HDFS store time: " + storeTime);
  }

  @Override
  public long selectSeriesCount() {
    return 0;
  }

  @Override
  public long selectDevicesCount() {
    return 0;
  }

  @Override
  public long selectDatabasesCount() {
    return 0;
  }

  private void saveTsFileInfo(TsFileInfo tsFileInfo) {
    Dataset<Row> dataset =
        spark
            .createDataset(List.of(tsFileInfo), Encoders.bean(TsFileInfo.class))
            .select("filePath", "fileVersion");
    saveDatasetToCsv(HdfsStorageConstants.FILES_DIRNAME, dataset);
  }

  private void saveDatasetToCsv(String dirname, Dataset<?> dataset) {
    dataset
        .repartition(partition)
        .write()
        .option("header", true)
        .option("compression", compression)
        .mode(SaveMode.Append)
        .csv(dirname);
  }

  private void saveTsFileStatsForPath(TsFileInfo tsFileInfo, Map.Entry<Path, TsFileStat> entry) {
    saveFileLevelStats(tsFileInfo, entry);
    saveChunkLevelStats(entry);
    savePageLevelStats(entry);
  }

  private void saveFileLevelStats(TsFileInfo info, Map.Entry<Path, TsFileStat> entry) {
    IoTDBSeriesStat fileStat = entry.getValue().getFileStat();
    FileLevelStat fileLevelStat =
        new FileLevelStat(
            fileStat.getVersion(), entry.getKey().getFullPath(), info.getFilePath(), fileStat);
    Dataset<FileLevelStat> dataset =
        spark.createDataset(List.of(fileLevelStat), Encoders.bean(FileLevelStat.class));
    saveDatasetToCsv(HdfsStorageConstants.FILE_SERIES_STATS_DIRNAME, dataset);
  }

  private void saveChunkLevelStats(Map.Entry<Path, TsFileStat> entry) {
    String path = entry.getKey().getFullPath();
    Map<Long, IoTDBSeriesStat> chunkStats = entry.getValue().getChunkStats();
    List<ChunkLevelStat> chunkLevelStats = new ArrayList<>();
    List<Long> offsets = chunkStats.keySet().stream().sorted().toList();
    for (Long offset : offsets) {
      IoTDBSeriesStat chunkStat = chunkStats.get(offset);
      ChunkLevelStat stat = new ChunkLevelStat(chunkStat.getVersion(), path, chunkStat, offset);
      chunkLevelStats.add(stat);
    }
    Dataset<ChunkLevelStat> dataset =
        spark.createDataset(chunkLevelStats, Encoders.bean(ChunkLevelStat.class));
    saveDatasetToCsv(HdfsStorageConstants.CHUNK_SERIES_STATS_DIRNAME, dataset);
  }

  private void savePageLevelStats(Map.Entry<Path, TsFileStat> entry) {
    String path = entry.getKey().getFullPath();
    Map<Long, List<IoTDBSeriesStat>> pageStats = entry.getValue().getPageStats();
    List<Long> offsets = pageStats.keySet().stream().sorted().toList();
    List<PageLevelStat> pageLevelStats = new ArrayList<>();
    for (int i = 0; i < offsets.size(); i++) {
      Long offset = offsets.get(i);
      List<IoTDBSeriesStat> stats = pageStats.get(offset);
      for (int j = 0; j < stats.size(); j++) {
        PageLevelStat stat =
            new PageLevelStat(
                stats.get(j).getVersion(),
                path,
                stats.get(j),
                String.format("%d-%d", i + 1, j + 1));
        pageLevelStats.add(stat);
      }
    }
    Dataset<PageLevelStat> dataset =
        spark.createDataset(pageLevelStats, Encoders.bean(PageLevelStat.class));
    saveDatasetToCsv(HdfsStorageConstants.PAGE_SERIES_STATS_DIRNAME, dataset);
  }

  @Override
  public List<IoTDBSeriesStat> selectSeriesStats(int pageIndex, int pageSize) {
    return null;
  }

  @Override
  public List<IoTDBSeriesStat> selectDeviceStats(int pageIndex, int pageSize) {
    return null;
  }

  @Override
  public List<IoTDBSeriesStat> selectDatabaseStats(int pageIndex, int pageSize) {
    return null;
  }

  @Override
  public IoTDBSeriesStat selectAllStats() {
    return null;
  }

  @Override
  public List<Double> getDataQuality(
      List<DQType> dqTypes, String path, List<TimeRange> timeRanges) {
    long start = System.currentTimeMillis();

    IoTDBSeriesStat fileLevelStat, chunkLevelStat = null;
    IoTDBSeriesStat pageLevelStat = null, originalDataStat = null;

    Dataset<Row> fileDataset =
        readDatasetFromCsv(HdfsStorageConstants.FILE_SERIES_STATS_DIRNAME, path, timeRanges);
    fileLevelStat = getStatFromDataset(fileDataset);
    List<TimeRange> fileLevelTimeRanges = getTimeRangesFromDataset(fileDataset);
    timeRanges = TimeRange.getRemains(timeRanges, fileLevelTimeRanges);

    if (!timeRanges.isEmpty()) {
      Dataset<Row> chunkDataset =
          readDatasetFromCsv(HdfsStorageConstants.CHUNK_SERIES_STATS_DIRNAME, path, timeRanges);
      chunkLevelStat = getStatFromDataset(chunkDataset);
      List<TimeRange> chunkLevelTimeRanges = getTimeRangesFromDataset(chunkDataset);
      timeRanges = TimeRange.getRemains(timeRanges, chunkLevelTimeRanges);
    }

    if (!timeRanges.isEmpty()) {
      Dataset<Row> pageDataset =
          readDatasetFromCsv(HdfsStorageConstants.PAGE_SERIES_STATS_DIRNAME, path, timeRanges);
      pageLevelStat = getStatFromDataset(pageDataset);
      List<TimeRange> pageLevelTimeRanges = getTimeRangesFromDataset(pageDataset);
      timeRanges = TimeRange.getRemains(timeRanges, pageLevelTimeRanges);
    }

    if (!timeRanges.isEmpty()) {
      originalDataStat = getStatFromOriginalData(sessionPool, path, timeRanges);
    }

    List<Double> result =
        mergeStatsAsDQMetrics(
            dqTypes, fileLevelStat, chunkLevelStat, pageLevelStat, originalDataStat);

    System.out.println("HDFS get data quality time: " + (System.currentTimeMillis() - start));
    return result;
  }

  private Dataset<Row> readDatasetFromCsv(
      String filePath, String seriesPath, List<TimeRange> timeRanges) {
    Dataset<Row> dataset = spark.read().option("header", true).csv(filePath);
    return dataset.filter(col("path").equalTo(seriesPath)).filter(getTimeFilter(timeRanges));
  }

  private IoTDBSeriesStat getStatFromDataset(Dataset<Row> dataset) {
    Row row = dataset.selectExpr(MetadataStat.statSumColumns()).first();
    return new IoTDBSeriesStat(row);
  }

  private List<TimeRange> getTimeRangesFromDataset(Dataset<Row> dataset) {
    return dataset.select("minTime", "maxTime").collectAsList().stream()
        .map(
            x ->
                new TimeRange(
                    Long.parseLong(x.getAs("minTime")), Long.parseLong(x.getAs("maxTime"))))
        .toList();
  }

  private Column getTimeFilter(List<TimeRange> timeRanges) {
    Column filter = lit(true);
    if (timeRanges == null) {
      return filter;
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
      filter = filter == lit(true) ? timeRangeCondition : filter.or(timeRangeCondition);
    }
    return filter;
  }
}
