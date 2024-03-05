package cn.edu.tsinghua.tsquality.preaggregation;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.mappers.database.ChunkTestMapper;
import cn.edu.tsinghua.tsquality.mappers.database.FileTestMapper;
import cn.edu.tsinghua.tsquality.mappers.database.SeriesTestMapper;
import cn.edu.tsinghua.tsquality.mappers.database.TableMapper;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

@SpringBootTest
public class PreAggregationTest {
  private static final int MAX_WAIT_SECONDS = 10;
  private static final int TEST_DATA_SIZE = 1000;

  @Autowired private IoTDBDataGenerator dataGenerator;
  @Autowired private TableMapper tableMapper;
  @Autowired private SeriesTestMapper seriesMapper;
  @Autowired private FileTestMapper fileMapper;
  @Autowired private ChunkTestMapper chunkMapper;

  @BeforeEach
  void insertData() throws Exception {
    dataGenerator.flush();
    tableMapper.truncateAllTables();
    dataGenerator.deleteDatabase();
    dataGenerator.generateData(TEST_DATA_SIZE);
    dataGenerator.flush();
  }

  @AfterEach
  void deleteData() throws Exception {
    dataGenerator.deleteDatabase();
  }

  @Test
  void testStatsGenerationShouldSucceed() {
    await().atMost(Duration.ofSeconds(MAX_WAIT_SECONDS)).until(statsGenerated());
  }

  private Callable<Boolean> statsGenerated() {
    return () ->
        seriesGeneratedSuccessfully()
            && filesGeneratedSuccessfully()
            && chunksGeneratedSuccessfully();
  }

  private boolean seriesGeneratedSuccessfully() {
    List<String> series = seriesMapper.selectAll();
    return seriesTableNotEmpty(series) && seriesContainsCorrectData(series);
  }

  private boolean seriesTableNotEmpty(List<String> series) {
    return series != null && !series.isEmpty();
  }

  private boolean seriesContainsCorrectData(List<String> series) {
    if (series.size() != IoTDBDataGenerator.SERIES_COUNT) {
      return false;
    }
    List<String> expectedSeries =
        IoTDBDataGenerator.paths.stream().map(Path::toString).toList();
    return series.containsAll(expectedSeries) && expectedSeries.containsAll(series);
  }

  private boolean filesGeneratedSuccessfully() {
    List<String> files = fileMapper.selectAllFilePaths();
    return files != null && !files.isEmpty();
  }

  private boolean chunksGeneratedSuccessfully() {
    List<Long> offsets = chunkMapper.selectAllChunkOffsets();
    return offsets != null && !offsets.isEmpty();
  }
}
