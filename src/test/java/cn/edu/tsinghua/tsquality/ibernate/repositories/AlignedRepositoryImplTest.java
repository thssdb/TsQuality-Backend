package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Collections;
import java.util.List;

@SpringBootTest
public class AlignedRepositoryImplTest {
  private static final List<Path> paths = IoTDBDataGenerator.getPaths();
  private static final List<TSDataType> dataTypes = IoTDBDataGenerator.getDataTypes();
  private AlignedRepositoryImpl underTests;

  @Autowired private SessionPool sessionPool;

  @BeforeEach
  void setup() {
    underTests = new AlignedRepositoryImpl(sessionPool, paths);
  }

  @AfterEach
  void clear() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteDatabase(IoTDBDataGenerator.getDATABASE_NAME());
  }

  @Test
  void testCreateAlignedTimeSeriesShouldSucceed() throws Exception {
    underTests.createAlignedTimeSeries(dataTypes);
    for (Path path : paths) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isTrue();
    }
  }

  @Test
  void testDeleteAlignedTimeSeriesShouldSucceed() throws Exception {
    underTests.createAlignedTimeSeries(dataTypes);
    underTests.deleteAlignedTimeSeries();
    for (Path path : paths) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isFalse();
    }
  }

  @Test
  void testInsertShouldNotThrow() throws Exception {
    long timestamp = 1000;
    List<Object> values = Collections.nCopies(paths.size(), 1.0);
    underTests.createAlignedTimeSeries(dataTypes);
    assertDoesNotThrow(() -> underTests.insert(timestamp, dataTypes, values));
  }

  @Test
  void testSelectShouldNotThrow() throws Exception {
    underTests.createAlignedTimeSeries(dataTypes);
    assertDoesNotThrow(() -> underTests.select(paths, null, null));
  }

  @Test
  void testSelectAfterInsertShouldReturnCorrectData() throws Exception {
    long timestamp = 1000;
    List<Object> values = Collections.nCopies(paths.size(), 1.0);
    List<List<Object>> result = whenSelectAfterInsert(timestamp, values);
    thenSelectResultShouldContainExactly(result, values);
  }

  private List<List<Object>> whenSelectAfterInsert(long timestamp, List<Object> values)
      throws Exception {
    underTests.createAlignedTimeSeries(dataTypes);
    underTests.insert(timestamp, dataTypes, values);
    return underTests.select(paths, null, null);
  }

  private void thenSelectResultShouldContainExactly(
      List<List<Object>> result, List<Object> values) {
    assertThat(result.size()).isEqualTo(1);
    assertThat(result).containsExactly(values);
  }
}
