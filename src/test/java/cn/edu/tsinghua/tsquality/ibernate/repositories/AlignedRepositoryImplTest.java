package cn.edu.tsinghua.tsquality.ibernate.repositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class AlignedRepositoryImplTest {
  private static final String DEVICE = IoTDBDataGenerator.getDEVICE();
  private static final List<Path> PATHS = IoTDBDataGenerator.getPaths();
  private static final List<TSDataType> DATA_TYPES = IoTDBDataGenerator.getDataTypes();
  private AlignedRepositoryImpl underTests;

  @Autowired private SessionPool sessionPool;

  @BeforeEach
  void setup() {
    underTests = new AlignedRepositoryImpl(sessionPool, PATHS);
  }

  @AfterEach
  void clear() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteDatabase(IoTDBDataGenerator.getDATABASE_NAME());
  }

  @Test
  void testCreateAlignedTimeSeriesShouldSucceed() throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    for (Path path : PATHS) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isTrue();
    }
  }

  @Test
  void testRepeatedlyCreateAlignedTimeSeriesShouldNotThrow() throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    assertDoesNotThrow(() -> underTests.createAlignedTimeSeries(DATA_TYPES));
    for (Path path : PATHS) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isTrue();
    }
  }

  @Test
  void testCountAfterFullInsertShouldReturnCorrectResult() throws Exception {
    long timestamp = 100;
    underTests.createAlignedTimeSeries(DATA_TYPES);
    List<String> measurements = givenFullInsertMeasurements();
    List<Object> values = givenFullInsertValues();
    underTests.insert(timestamp, measurements, values);

    assertThat(underTests.count()).isEqualTo(1);
  }

  @Test
  void testCountTimeSeriesLikeShouldReturnCorrectResult() throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    String device = underTests.getDevice();
    assertThat(underTests.countTimeSeriesLike(device)).isEqualTo(PATHS.size());
  }

  @Test
  void testDeleteAlignedTimeSeriesShouldSucceed() throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    underTests.deleteAlignedTimeSeries();
    for (Path path : PATHS) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isFalse();
    }
  }

  @Test
  void testFullInsertShouldNotThrow() throws Exception {
    long timestamp = 1000;
    underTests.createAlignedTimeSeries(DATA_TYPES);

    List<String> measurements = givenFullInsertMeasurements();
    List<Object> values = givenFullInsertValues();
    assertDoesNotThrow(() -> underTests.insert(timestamp, measurements, values));
  }

  @Test
  void testPartialInsertShouldNotThrow() throws Exception {
    long timestamp = 1000;
    underTests.createAlignedTimeSeries(DATA_TYPES);

    List<String> measurements = givenPartialInsertMeasurements();
    List<Object> values = givenPartialInsertValues();
    assertDoesNotThrow(() -> underTests.insert(timestamp, measurements, values));
  }

  @Test
  void testInsertShouldNotThrow() throws Exception {
    long timestamp = 1000;
    underTests.createAlignedTimeSeries(DATA_TYPES);

    List<Object> values = givenFullInsertValues();
    assertDoesNotThrow(() -> underTests.insert(timestamp, values));
  }

  @Test
  void testSelectShouldNotThrow() throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    assertDoesNotThrow(() -> underTests.select(null, null));
  }

  @Test
  void testSelectAfterFullInsertShouldReturnCorrectResult() throws Exception {
    long timestamp = 1000;
    List<String> measurements = givenFullInsertMeasurements();
    List<Object> values = givenFullInsertValues();
    List<List<Object>> result = whenSelectAfterFullInsert(timestamp, measurements, values);
    thenSelectResultOfFullInsertShouldContainExactly(result, values);
  }

  @Test
  void testSelectAfterPartialInsertShouldReturnCorrectResult() throws Exception {
    long timestamp = 1000;
    List<String> measurements = givenPartialInsertMeasurements();
    List<Object> values = givenPartialInsertValues();
    List<List<Object>> result = whenSelectAfterPartialInsert(timestamp, measurements, values);
    thenSelectResultOfPartialInsertShouldContain(result, values);
  }

  @Test
  void testSelectAfterInsertShouldReturnCorrectResult() throws Exception {
    long timestamp = 1000;
    List<Object> values = givenFullInsertValues();
    List<List<Object>> result = whenSelectAfterInsert(timestamp, values);
    thenSelectResultOfFullInsertShouldContainExactly(result, values);
  }

  private List<Object> givenFullInsertValues() {
    return Collections.nCopies(PATHS.size(), 1.0);
  }

  private List<String> givenFullInsertMeasurements() {
    return PATHS.stream().map(Path::getMeasurement).toList();
  }

  private List<Object> givenPartialInsertValues() {
    return List.of(1.0);
  }

  private List<String> givenPartialInsertMeasurements() {
    return PATHS.subList(0, 1).stream().map(Path::getMeasurement).toList();
  }

  private List<List<Object>> whenSelectAfterFullInsert(
      long timestamp, List<String> measurements, List<Object> values) throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    underTests.insert(timestamp, measurements, values);
    return underTests.select(null, null);
  }

  private List<List<Object>> whenSelectAfterPartialInsert(
      long timestamp, List<String> measurements, List<Object> values) throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    underTests.insert(timestamp, measurements, values);
    return underTests.select(null, null);
  }

  private List<List<Object>> whenSelectAfterInsert(long timestamp, List<Object> values)
      throws Exception {
    underTests.createAlignedTimeSeries(DATA_TYPES);
    underTests.insert(timestamp, values);
    return underTests.select(null, null);
  }

  private void thenSelectResultOfFullInsertShouldContainExactly(
      List<List<Object>> result, List<Object> values) {
    assertThat(result.size()).isEqualTo(1);
    assertThat(result).containsExactly(values);
  }

  private void thenSelectResultOfPartialInsertShouldContain(
      List<List<Object>> result, List<Object> values) {
    assertThat(result.size()).isEqualTo(1);
    List<Object> row = result.getFirst();
    for (int i = 0; i < result.size(); i++) {
      if (i < values.size()) {
        assertThat(row.get(i)).isEqualTo(values.get(i));
      } else {
        assertThat(row.get(i)).isNull();
      }
    }
  }
}
