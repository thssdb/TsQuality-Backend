package cn.edu.tsinghua.tsquality.storage.iotdb;

import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.IoTDBStorageEngine;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

@SpringBootTest
public class IoTDBStorageEngineTest {
  @Autowired private IoTDBStorageEngine underTests;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;

  @Test
  void testSaveTsFileStatShouldSucceed() {
    TsFileInfo tsFileInfo = givenTsFileInfo();
    Map<Path, TsFileStat> stats = givenTsFileStats();
    assertDoesNotThrow(() -> underTests.saveTsFileStats(tsFileInfo, stats));
  }

  private TsFileInfo givenTsFileInfo() {
    return tsFileInfoGenerator.tsFileInfo();
  }

  private Map<Path, TsFileStat> givenTsFileStats() {
    return seriesStatGenerator.tsFileStats();
  }
}
