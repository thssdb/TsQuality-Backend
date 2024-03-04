package cn.edu.tsinghua.tsquality.storage.iotdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.iotdb.IoTDBStorageEngine;
import cn.edu.tsinghua.tsquality.storage.impl.iotdb.StatsTimeSeriesUtil;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IoTDBStorageEngineTest {
  @Autowired private SessionPool sessionPool;
  @Autowired private IoTDBStorageEngine underTests;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;
  @Autowired private StatsTimeSeriesUtil statsTimeSeriesUtil;

  @AfterEach
  void clear() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteDatabase(IoTDBDataGenerator.getDATABASE_NAME());
  }

  @Test
  void testSaveTsFileStatShouldNotThrow() {
    TsFileInfo tsFileInfo = givenTsFileInfo();
    Map<Path, TsFileStat> stats = givenTsFileStats();
    assertDoesNotThrow(() -> underTests.saveTsFileStats(tsFileInfo, stats));
  }

  @Test
  void testSaveTsFileStatShouldPersistResultInIoTDB() throws Exception {
    TsFileInfo tsFileInfo = givenTsFileInfo();
    Map<Path, TsFileStat> stats = givenTsFileStats();
    underTests.saveTsFileStats(tsFileInfo, stats);
    thenResultShouldHaveBeenPersisted(tsFileInfo, stats);
  }

  private void thenResultShouldHaveBeenPersisted(TsFileInfo info, Map<Path, TsFileStat> stats)
      throws Exception {
    thenFileStatsShouldHaveBeenPersisted(info, stats);
    thenChunkStatsShouldHaveBeenPersisted(stats);
  }

  private void thenFileStatsShouldHaveBeenPersisted(TsFileInfo info, Map<Path, TsFileStat> stats)
      throws Exception {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      thenFileStatsShouldHaveBeenPersistedForPath(info, entry);
    }
  }

  private void thenFileStatsShouldHaveBeenPersistedForPath(
      TsFileInfo info, Map.Entry<Path, TsFileStat> entry) throws Exception {
    AlignedRepository repository = fileStatsAlignedRepositoryForPath(entry.getKey());
    thenNumberOfFileStatsShouldBeOne(repository);
    thenFileStatsValuesShouldBe(repository, info.getFilePath(), entry.getValue().getFileStat());
  }

  private AlignedRepository fileStatsAlignedRepositoryForPath(Path path) {
    String device = statsTimeSeriesUtil.getFileStatsDeviceForPath(path);
    List<String> measurements = statsTimeSeriesUtil.getFileStatsMeasurementsForPath(path);
    return new AlignedRepositoryImpl(sessionPool, device, measurements);
  }

  private void thenNumberOfFileStatsShouldBeOne(AlignedRepository repository) throws Exception {
    assertThat(repository.count()).isEqualTo(1);
  }

  private void thenFileStatsValuesShouldBe(
      AlignedRepository repository, String filePath, IoTDBSeriesStat stat) throws Exception {
    List<List<Object>> result = repository.select(null, null);
    assertThat(result.size()).isEqualTo(1);
    List<Object> values = result.getFirst();
    assertThat(values.size()).isEqualTo(12); // 11 stat column + 1 file path column
    assertThat(values.getFirst().toString()).isEqualTo(filePath);
    thenStatsValuesShouldBe(values, stat);
  }

  private void thenStatsValuesShouldBe(List<Object> values, IoTDBSeriesStat stat) throws Exception {
    assertThat(values.get(1)).isEqualTo(stat.getMinTimestamp());
    assertThat(values.get(2)).isEqualTo(stat.getMaxTimestamp());
    assertThat(values.get(3)).isEqualTo(stat.getCnt());
    assertThat(values.get(4)).isEqualTo(stat.getMissCnt());
    assertThat(values.get(5)).isEqualTo(stat.getSpecialCnt());
    assertThat(values.get(6)).isEqualTo(stat.getLateCnt());
    assertThat(values.get(7)).isEqualTo(stat.getRedundancyCnt());
    assertThat(values.get(8)).isEqualTo(stat.getValueCnt());
    assertThat(values.get(9)).isEqualTo(stat.getVariationCnt());
    assertThat(values.get(10)).isEqualTo(stat.getSpeedCnt());
    assertThat(values.get(11)).isEqualTo(stat.getValueCnt());
  }

  private void thenChunkStatsShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) throws Exception {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      thenChunkStatsShouldHaveBeenPersistedForPath(entry);
    }
  }

  private void thenChunkStatsShouldHaveBeenPersistedForPath(Map.Entry<Path, TsFileStat> entry)
      throws Exception {
    AlignedRepository repository = chunkStatsAlignedRepositoryForPath(entry.getKey());
    thenNumberOfChunkStatsShouldBeTheSizeOf(repository, entry.getValue().getChunkStats());
    thenChunkStatsValuesShouldBe(repository, entry.getValue().getChunkStats());
  }

  private AlignedRepository chunkStatsAlignedRepositoryForPath(Path path) {
    String device = statsTimeSeriesUtil.getChunkStatsDeviceForPath(path);
    List<String> measurements = statsTimeSeriesUtil.getChunkStatsMeasurementsForPath(path);
    return new AlignedRepositoryImpl(sessionPool, device, measurements);
  }

  private void thenNumberOfChunkStatsShouldBeTheSizeOf(
      AlignedRepository repository, Map<Long, IoTDBSeriesStat> chunkStats) throws Exception {
    assertThat(repository.count()).isEqualTo(chunkStats.size());
  }

  private void thenChunkStatsValuesShouldBe(
      AlignedRepository repository, Map<Long, IoTDBSeriesStat> stats) throws Exception {
    List<List<Object>> result = repository.select(null, null);
    assertThat(result.size()).isEqualTo(stats.size());
    Set<Long> offsets = stats.keySet();
    for (List<Object> values : result) {
      assertThat(values.size()).isEqualTo(12);
      assertThat(values.getFirst()).isIn(offsets);
      thenStatsValuesShouldBe(values, stats.get((Long) values.getFirst()));
    }
  }

  private TsFileInfo givenTsFileInfo() {
    return tsFileInfoGenerator.tsFileInfo();
  }

  private Map<Path, TsFileStat> givenTsFileStats() {
    return seriesStatGenerator.tsFileStats();
  }
}
