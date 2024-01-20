package cn.edu.tsinghua.tsquality.services;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import cn.edu.tsinghua.tsquality.service.impl.TimestampAnomalyServiceImpl;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
public class TimestampAnomalyServiceImplTest {
  public static final int TEST_DATA_SIZE = 100;

  @Autowired private IoTDBDataGenerator dataGenerator;
  @Autowired private TimestampAnomalyServiceImpl underTests;

  @BeforeEach
  void insertDataWithTimestampAnomalies() throws IoTDBConnectionException {
    dataGenerator.generateTimestampAnomalyData(TEST_DATA_SIZE);
  }

  @AfterEach
  void clearData() throws IoTDBConnectionException {
    dataGenerator.deleteAll();
  }

  @Test
  void testAnomalyDetectionAndRepairWithNoArgs() {
    String path = dataGenerator.getPaths()[0].getFullPath();
    List<TimestampAnomalyDto> result = underTests.anomalyDetectionAndRepair(path);
    assertThat(result).isNotNull();
  }

  @Test
  void testAnomalyDetectionAndRepairWithStandardInterval() {}

  @Test
  void testAnomalyDetectionAndRepairWithDetectionMethod() {}
}
