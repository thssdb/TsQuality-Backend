package cn.edu.tsinghua.tsquality.storage.iotdb;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.iotdb.IoTDBStorageEngine;
import java.util.Map;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IoTDBStorageEngineIntegrationTest {
  @Autowired private IoTDBStorageEngine underTests;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;

  @Test
  void testSelectAfterSaveShouldReturnCorrectResult() {
    TsFileInfo tsFileInfo = givenTsFileInfo();
    Map<Path, TsFileStat> tsFileStats = givenTsFileStats();
    IoTDBSeriesStat stat = whenSelectAllStatsAfterSave(tsFileInfo, tsFileStats);
    thenSelectResultShouldBeCorrect(stat);
  }

  private TsFileInfo givenTsFileInfo() {
    return tsFileInfoGenerator.tsFileInfo();
  }

  private Map<Path, TsFileStat> givenTsFileStats() {
    return seriesStatGenerator.tsFileStats();
  }

  private IoTDBSeriesStat whenSelectAllStatsAfterSave(
      TsFileInfo tsFileInfo, Map<Path, TsFileStat> tsFileStats) {
    underTests.saveTsFileStats(tsFileInfo, tsFileStats);
    return underTests.selectAllStats();
  }

  private void thenSelectResultShouldBeCorrect(IoTDBSeriesStat stat) {
    assertThat(stat).isNotNull();
    assertThat(stat.getCnt()).isEqualTo(SeriesStatGenerator.CNT_PER_FILE);
  }
}
