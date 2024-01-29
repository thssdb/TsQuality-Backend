package cn.edu.tsinghua.tsquality.services;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.impl.TimestampAnomalyServiceImpl;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TimestampAnomalyServiceImplTest {
  private static final int TEST_DATA_SIZE = 100;

  @Autowired private TimestampAnomalyServiceImpl underTests;
  @Autowired private IoTDBDataGenerator dataGenerator;

  private String path;

  @BeforeEach
  void insertDataWithTimestampAnomalies() throws Exception {
    dataGenerator.generateTimestampAnomalyData(TEST_DATA_SIZE);
    path = dataGenerator.getPaths().getFirst().getFullPath();
  }

  @AfterEach
  void clearData() throws Exception {
    dataGenerator.deleteAll();
  }

  @Test
  void testAnomalyDetectionAndRepairWithNoArgs() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path);
    thenResultDataSizeShouldBeCorrect(result);
  }

  @Test
  void testAnomalyDetectionAndRepairWithStandardInterval() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, 1000L);
    thenResultDataSizeShouldBeCorrect(result);
  }

  @Test
  void testAnomalyDetectionAndRepairWithDetectionMethod() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, "mode");
    thenResultDataSizeShouldBeCorrect(result);
  }

  private void thenResultDataSizeShouldBeCorrect(TimestampAnomalyResultDto result) {
    assertThat(result.getOriginalData().size()).isEqualTo(TEST_DATA_SIZE);
    assertThat(result.getRepairedData().size()).isGreaterThan(0);
  }
}
