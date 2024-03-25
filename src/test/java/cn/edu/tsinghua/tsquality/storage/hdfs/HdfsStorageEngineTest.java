package cn.edu.tsinghua.tsquality.storage.hdfs;

import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.HdfsStorageConstants;
import cn.edu.tsinghua.tsquality.storage.impl.hdfs.HdfsStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.spark.sql.SparkSession;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class HdfsStorageEngineTest {
  @Autowired private HdfsStorageEngine underTests;
  @Autowired private SparkSession spark;
  @Autowired private Configuration conf;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;

  private static boolean checkDirExists(String dirName, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return fs.exists(new org.apache.hadoop.fs.Path(dirName));
  }

  @Test
  void testSaveTsFileStatShouldPersistResultInHdfs() throws Exception {
    TsFileInfo tsFileInfo = tsFileInfoGenerator.tsFileInfo();
    Map<Path, TsFileStat> stats = seriesStatGenerator.tsFileStats();
    underTests.saveTsFileStats(tsFileInfo, stats);
    thenResultShouldHaveBeenPersisted(tsFileInfo, stats);
  }

  private void thenResultShouldHaveBeenPersisted(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats)
      throws Exception {
    thenFileSeriesStatsShouldHaveBeenPersisted(tsFileInfo, stats);
    thenChunkSeriesStatsShouldHaveBeenPersisted(tsFileInfo, stats);
    thenPageSeriesStatsShouldHaveBeenPersisted(tsFileInfo, stats);
  }

  private void thenFileSeriesStatsShouldHaveBeenPersisted(
      TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) throws Exception {
    assertThat(checkDirExists(HdfsStorageConstants.fileSeriesStatsDirName, conf))
        .isTrue();
  }

  private void thenChunkSeriesStatsShouldHaveBeenPersisted(
      TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) throws Exception {
    assertThat(checkDirExists(HdfsStorageConstants.chunkSeriesStatsDirName, conf))
        .isTrue();
  }


  private void thenPageSeriesStatsShouldHaveBeenPersisted(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats) throws IOException {
    assertThat(checkDirExists(HdfsStorageConstants.pageSeriesStatsDirName, conf))
        .isTrue();
  }
}
