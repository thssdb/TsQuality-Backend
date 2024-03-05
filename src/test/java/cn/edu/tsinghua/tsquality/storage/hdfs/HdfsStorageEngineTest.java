package cn.edu.tsinghua.tsquality.storage.hdfs;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.HdfsStorageConstants;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.HdfsStorageEngine;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.util.HdfsUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class HdfsStorageEngineTest {
  @Autowired private HdfsStorageEngine underTests;
  @Autowired private SparkSession spark;
  @Autowired private Configuration conf;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;

  @Test
  void testSaveTsFileStatShouldPersistResultInHdfs() throws Exception {
    TsFileInfo tsFileInfo = tsFileInfoGenerator.tsFileInfo();
    Map<Path, TsFileStat> stats = seriesStatGenerator.tsFileStats();
    underTests.saveTsFileStats(tsFileInfo, stats);
    thenResultShouldHaveBeenPersisted(tsFileInfo, stats);
  }

  private void thenResultShouldHaveBeenPersisted(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats)
      throws Exception {
    thenSeriesShouldHaveBeenPersisted(stats);
    thenFilesShouldHaveBeenPersisted(tsFileInfo);
    thenChunksShouldHaveBeenPersisted(stats);
    thenFileSeriesStatsShouldHaveBeenPersisted(tsFileInfo, stats);
    thenChunkSeriesStatsShouldHaveBeenPersisted(tsFileInfo, stats);
  }

  private void thenSeriesShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) throws Exception {
    assertThat(HdfsUtils.checkFileExists(HdfsStorageConstants.seriesFilePath, conf)).isTrue();
    Dataset<Row> dataset =
        spark.read().option("header", true).csv(HdfsStorageConstants.seriesFilePath);
    assertThat(dataset.count()).isEqualTo(SeriesStatGenerator.SERIES_PER_FILE);
    List<String> paths = dataset.select("path").as(Encoders.STRING()).collectAsList();
    assertThat(paths)
        .containsExactlyElementsOf(
            SeriesStatGenerator.PATHS.stream().map(Path::getFullPath).toList());
  }

  private void thenFilesShouldHaveBeenPersisted(TsFileInfo tsFileInfo) throws Exception {
    assertThat(HdfsUtils.checkFileExists(HdfsStorageConstants.filesFilePath, conf)).isTrue();
  }

  private void thenChunksShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) throws Exception {
    assertThat(HdfsUtils.checkFileExists(HdfsStorageConstants.chunksFilePath, conf)).isTrue();
  }

  private void thenFileSeriesStatsShouldHaveBeenPersisted(
      TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) throws Exception {
    assertThat(HdfsUtils.checkFileExists(HdfsStorageConstants.fileSeriesStatsFilePath, conf))
        .isTrue();
  }

  private void thenChunkSeriesStatsShouldHaveBeenPersisted(
      TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) throws Exception {
    assertThat(HdfsUtils.checkFileExists(HdfsStorageConstants.chunkSeriesStatsFilePath, conf))
        .isTrue();
  }
}
