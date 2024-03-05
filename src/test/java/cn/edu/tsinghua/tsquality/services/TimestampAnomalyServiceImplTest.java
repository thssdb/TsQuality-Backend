package cn.edu.tsinghua.tsquality.services;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.generators.TimestampGenerator;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.timeseries.impl.TimestampAnomalyServiceImpl;
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
    path = IoTDBDataGenerator.paths.getFirst().getFullPath();
  }

  @AfterEach
  void clearData() throws Exception {
    dataGenerator.deleteDatabase();
  }

  @Test
  void testAnomalyDetectionAndRepairWithNoArgs() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, null);
    thenOriginalDataSizeShouldBe(result, TEST_DATA_SIZE);
    thenRepairedDataSizeShouldBeGreaterThanZero(result);
  }

  @Test
  void testAnomalyDetectionAndRepairWithStandardInterval() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, 1000L, null);
    thenOriginalDataSizeShouldBe(result, TEST_DATA_SIZE);
    thenRepairedDataSizeShouldBeGreaterThanZero(result);
  }

  @Test
  void testAnomalyDetectionAndRepairWithDetectionMethod() {
    TimestampAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, "mode", null);
    thenOriginalDataSizeShouldBe(result, TEST_DATA_SIZE);
    thenRepairedDataSizeShouldBeGreaterThanZero(result);
  }

  @Test
  void testAnomalyDetectionAndRepairWithTimeFilter() {
    long timestamp =
        TimestampGenerator.START_TIMESTAMP + (TEST_DATA_SIZE / 2) * TimestampGenerator.INTERVAL;
    TimestampAnomalyResultDto result =
        underTests.anomalyDetectionAndRepair(path, "time < " + timestamp);
    thenOriginalDataSizeShouldBe(result, TEST_DATA_SIZE / 2 + 1);
    thenRepairedDataSizeShouldBeGreaterThanZero(result);
  }

  private void thenOriginalDataSizeShouldBe(TimestampAnomalyResultDto result, int size) {
    assertThat(result.getOriginalData().size()).isEqualTo(size);
  }

  private void thenRepairedDataSizeShouldBeGreaterThanZero(TimestampAnomalyResultDto result) {
    assertThat(result.getRepairedData().size()).isGreaterThan(0);
  }
}
