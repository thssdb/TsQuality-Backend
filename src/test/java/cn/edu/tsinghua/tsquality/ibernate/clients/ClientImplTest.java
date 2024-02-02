package cn.edu.tsinghua.tsquality.ibernate.clients;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.ibernate.clients.impl.ClientImpl;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ClientImplTest {
  private static final int TEST_DATA_SIZE = 100;

  @Autowired private ClientImpl underTests;
  @Autowired private IoTDBDataGenerator dataGenerator;

  @BeforeEach
  void insertData() throws IoTDBConnectionException, StatementExecutionException {
    dataGenerator.generateData(TEST_DATA_SIZE);
  }

  @AfterEach
  void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    dataGenerator.deleteDatabase();
  }

  @Test
  void testCountTimeSeriesShouldSucceed() {
    long count = underTests.countTimeSeries();
    assertThat(count).isEqualTo(IoTDBDataGenerator.SERIES_COUNT);
  }

  @Test
  void testCountDevicesShouldSucceed() {
    long count = underTests.countDevices();
    assertThat(count).isEqualTo(IoTDBDataGenerator.DEVICE_COUNT);
  }

  @Test
  void testCountDatabasesShouldSucceed() {
    long count = underTests.countDatabases();
    assertThat(count).isEqualTo(IoTDBDataGenerator.DATABASE_COUNT);
  }
}
