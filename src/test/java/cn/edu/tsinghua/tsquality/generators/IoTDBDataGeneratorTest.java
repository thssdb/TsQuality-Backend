package cn.edu.tsinghua.tsquality.generators;

import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IoTDBDataGeneratorTest {
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
    int size = 10;
    underTests.generateTimestampAnomalyData(size);
    for (Path path: underTests.getPaths()) {
      DataIterator result = whenQueryData(path);
      thenCountResultShouldBeOfSize(result, size);
    }
  }

  @Test
  void testDeleteAllAfterGenerationShouldSucceed() throws Exception {
    underTests.generateTimestampAnomalyData(10);
    underTests.deleteAll();
    for (Path path: underTests.getPaths()) {
      DataIterator result = whenQueryData(path);
      thenCountResultShouldBeEmpty(result);
    }
  }

  private DataIterator whenQueryData(Path path) throws Exception {
    session.open();
    String device = path.getDevice();
    String measurement = path.getMeasurement();
    return session.executeQueryStatement(String.format("select count(%s) from %s", measurement, device)).iterator();
  }

  private void thenCountResultShouldBeOfSize(DataIterator result, int size) throws Exception {
    assertThat(result.next()).isTrue();
    assertThat(result.getLong(1)).isEqualTo(size);
  }

  private void thenCountResultShouldBeEmpty(DataIterator result) throws Exception {
    assertThat(result.next()).isFalse();
  }
}
