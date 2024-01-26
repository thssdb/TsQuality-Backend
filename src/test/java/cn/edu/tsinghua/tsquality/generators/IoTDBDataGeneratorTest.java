package cn.edu.tsinghua.tsquality.generators;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IoTDBDataGeneratorTest {
  public static final int TEST_DATA_SIZE = 100;

  @Autowired private IoTDBDataGenerator underTests;

  private final Session session;

  public IoTDBDataGeneratorTest() {
    session = new Session.Builder().build();
  }

  @AfterEach
  void closeSession() throws IoTDBConnectionException {
    session.close();
    underTests.deleteAll();
  }

  @Test
  void testGenerateDataShouldSucceed() throws Exception {
    underTests.generateTimestampAnomalyData(TEST_DATA_SIZE);
    for (Path path : underTests.getPaths()) {
      DataIterator result = whenQueryData(path);
      thenCountResultShouldBeOfSize(result, TEST_DATA_SIZE);
    }
  }

  @Test
  void testDeleteAllAfterGenerationShouldSucceed() throws Exception {
    underTests.generateTimestampAnomalyData(TEST_DATA_SIZE);
    underTests.deleteAll();
    for (Path path : underTests.getPaths()) {
      DataIterator result = whenQueryData(path);
      thenCountResultShouldBeEmpty(result);
    }
  }

  private DataIterator whenQueryData(Path path) throws Exception {
    session.open();
    String device = path.getDevice();
    String measurement = path.getMeasurement();
    return session
        .executeQueryStatement(String.format("select count(%s) from %s", measurement, device))
        .iterator();
  }

  private void thenCountResultShouldBeOfSize(DataIterator result, int size) throws Exception {
    assertThat(result.next()).isTrue();
    assertThat(result.getLong(1)).isEqualTo(size);
  }

  private void thenCountResultShouldBeEmpty(DataIterator result) throws Exception {
    assertThat(result.next()).isFalse();
  }
}
