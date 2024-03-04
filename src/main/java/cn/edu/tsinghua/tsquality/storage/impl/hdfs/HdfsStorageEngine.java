package cn.edu.tsinghua.tsquality.storage.impl.hdfs;


import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("hdfsStorageEngine")
public class HdfsStorageEngine implements MetadataStorageEngine {
  private final Configuration conf;
  private final SparkSession spark;

  public HdfsStorageEngine(Configuration config, SparkSession spark) throws IOException {
    this.conf = config;
    this.spark = spark;
    createMetadataDirIfNotExists();
  }

  private void createMetadataDirIfNotExists() throws IOException {
    FileSystem.get(conf).mkdirs(new org.apache.hadoop.fs.Path(HdfsStorageConstants.metadataDirname));
  }

  @Override
  public void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) {
    saveSeries(stats.keySet(), tsFileInfo.getDatabase());
  }

  private void saveSeries(Set<Path> paths, String database) {
    List<IoTDBSeries> series = IoTDBSeries.fromPaths(paths.stream().toList(), database);
    Dataset<IoTDBSeries> dataset = spark.createDataset(series, Encoders.bean(IoTDBSeries.class));
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
