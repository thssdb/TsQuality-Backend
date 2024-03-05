package cn.edu.tsinghua.tsquality.services;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.ValueAnomalyResultDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.ValueRepairedDataPointDto;
import cn.edu.tsinghua.tsquality.service.timeseries.impl.ValueAnomalyServiceImpl;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ValueAnomalyServiceImplTest {
  private static final Double[] anomalyData =
      new Double[] {
        100.0,
        101.0,
        102.0,
        104.0,
        126.0,
        108.0,
        112.0,
        113.0,
        114.0,
        116.0,
        118.0,
        100.0,
        124.0,
        126.0,
        Double.NaN
      };
  private static final Double[] repairedData =
      new Double[] {
        100.0, 101.0, 102.0, 104.0, 106.0, 108.0, 112.0, 113.0, 114.0, 116.0, 118.0, 120.0, 124.0,
        126.0, 128.0
      };

  @Autowired private ValueAnomalyServiceImpl underTests;
  @Autowired private IoTDBDataGenerator dataGenerator;

  private String path;

  @BeforeEach
  void insertDataWithValueAnomalies() throws Exception {
    dataGenerator.generateValueAnomalyData(anomalyData);
    path = IoTDBDataGenerator.paths.getFirst().getFullPath();
  }

  @AfterEach
  void clearData() throws Exception {
    dataGenerator.deleteDatabase();
  }

  @Test
  void testValueAnomalyDetectionAndRepairWithNoArgs() {
    ValueAnomalyResultDto result = underTests.anomalyDetectionAndRepair(path, null);
    thenResultShouldBeRepairedCorrectly(result);
  }

  private void thenResultShouldBeRepairedCorrectly(ValueAnomalyResultDto result) {
    List<ValueRepairedDataPointDto> data = result.getData();
    assertThat(data.size()).isEqualTo(anomalyData.length);

    boolean repaired = false;
    for (int i = 0; i < data.size(); i++) {
      ValueRepairedDataPointDto point = data.get(i);
      assertThat(point.getOriginal()).isEqualTo(anomalyData[i]);
      if (!point.getOriginal().equals(point.getRepaired())) {
        repaired = true;
      }
    }
    assertThat(repaired).isTrue();
  }
}
