package cn.edu.tsinghua.tsquality.storage.rdbms;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.SeriesStatGenerator;
import cn.edu.tsinghua.tsquality.generators.TsFileInfoGenerator;
import cn.edu.tsinghua.tsquality.mappers.database.*;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import cn.edu.tsinghua.tsquality.storage.impl.RdbmsStorageEngine;
import java.util.List;
import java.util.Map;
import lombok.val;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
public class RdbmsStorageEngineTest {
  @Autowired private RdbmsStorageEngine underTests;
  @Autowired private TsFileInfoGenerator tsFileInfoGenerator;
  @Autowired private SeriesStatGenerator seriesStatGenerator;

  @Autowired private IoTDBSeriesMapper seriesMapper;
  @Autowired private IoTDBFileMapper fileMapper;
  @Autowired private IoTDBChunkMapper chunkMapper;
  @Autowired private IoTDBFileSeriesStatMapper fileSeriesStatMapper;
  @Autowired private IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;

  @Test
  void testSaveTsFileStatsShouldPersistResultInIoTDB() {
    TsFileInfo info = tsFileInfoGenerator.tsFileInfo();
    Map<Path, TsFileStat> stats = seriesStatGenerator.tsFileStats();
    underTests.saveTsFileStats(info, stats);

    thenSeriesShouldHaveBeenPersisted(stats);
    thenFilesShouldHaveBeenPersisted(info);
    thenFileStatsShouldHaveBeenPersisted(stats);
    thenChunksShouldHaveBeenPersisted(stats);
    thenChunkStatsShouldHaveBeenPersisted(stats);
  }

  private void thenSeriesShouldHaveBeenPersisted(Map<Path, TsFileStat> stat) {
    List<String> paths = seriesMapper.select().stream().map(IoTDBSeries::getPath).toList();
    assertThat(paths).hasSameSizeAs(stat.keySet());
    for (val path : stat.keySet()) {
      assertThat(paths).contains(path.getFullPath());
    }
  }

  private void thenFilesShouldHaveBeenPersisted(TsFileInfo info) {
    List<IoTDBFile> files = fileMapper.select();
    assertThat(files).hasSize(1);
    IoTDBFile file = files.getFirst();
    assertThat(file.getFileVersion()).isEqualTo(info.getFileVersion());
    assertThat(file.getFilePath()).isEqualTo(info.getFilePath());
  }

  private void thenFileStatsShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      thenFileStatsShouldHaveBeenPersistedForPath(entry);
    }
  }

  private void thenFileStatsShouldHaveBeenPersistedForPath(Map.Entry<Path, TsFileStat> entry) {
    List<IoTDBSeriesStat> stats = fileSeriesStatMapper.selectByPath(entry.getKey().getFullPath());
    assertThat(stats.size()).isEqualTo(1);
    assertThat(stats.getFirst())
        .usingRecursiveComparison()
        .ignoringFields("path")
        .ignoringFields("timeList")
        .ignoringFields("valueList")
        .isEqualTo(entry.getValue().getFileStat());
  }

  private void thenChunksShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      thenChunksShouldHaveBeenPersistedForPath(entry);
    }
  }

  private void thenChunksShouldHaveBeenPersistedForPath(Map.Entry<Path, TsFileStat> entry) {
    List<IoTDBChunk> chunks = chunkMapper.selectByPath(entry.getKey().getFullPath());
    Map<Long, IoTDBSeriesStat> stats = entry.getValue().getChunkStats();
    assertThat(chunks).hasSameSizeAs(stats.keySet());
    for (val chunk : chunks) {
      assertThat(stats).containsKey(chunk.getOffset());
    }
  }

  private void thenChunkStatsShouldHaveBeenPersisted(Map<Path, TsFileStat> stats) {
    for (Map.Entry<Path, TsFileStat> entry : stats.entrySet()) {
      thenChunkStatsShouldHaveBeenPersistedForPath(entry);
    }
  }

  private void thenChunkStatsShouldHaveBeenPersistedForPath(Map.Entry<Path, TsFileStat> entry) {
    Map<Long, IoTDBSeriesStat> expectedChunkStats = entry.getValue().getChunkStats();
    List<IoTDBSeriesStat> actualChunkStats =
        chunkSeriesStatMapper.selectByPath(entry.getKey().getFullPath());
    assertThat(actualChunkStats).hasSameSizeAs(expectedChunkStats.keySet());

    List<IoTDBChunk> chunks = chunkMapper.selectByPath(entry.getKey().getFullPath());
    assertThat(actualChunkStats).hasSameSizeAs(chunks);

    for (int i = 0; i < chunks.size(); i++) {
      long offset = chunks.get(i).getOffset();
      assertThat(expectedChunkStats).containsKey(offset);
      assertThat(actualChunkStats.get(i))
          .usingRecursiveComparison()
          .ignoringFields("path")
          .ignoringFields("timeList")
          .ignoringFields("valueList")
          .isEqualTo(expectedChunkStats.get(offset));
    }
  }
}
