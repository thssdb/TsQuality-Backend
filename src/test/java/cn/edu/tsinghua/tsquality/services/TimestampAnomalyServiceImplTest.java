package cn.edu.tsinghua.tsquality.services;

import cn.edu.tsinghua.tsquality.service.impl.TimestampAnomalyServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
public class TimestampAnomalyServiceImplTest {
  @Autowired private TimestampAnomalyServiceImpl underTests;

  @Test
  void testAnomalyDetectionAndRepairWithNoArgs() {

  }

  @Test
  void testAnomalyDetectionAndRepairWithStandardInterval() {

  }

  @Test
  void testAnomalyDetectionAndRepairWithDetectionMethod() {

  }

  @Test
  void testAnomalyDetectionAndRepairWithFullArgs() {

  }
}
