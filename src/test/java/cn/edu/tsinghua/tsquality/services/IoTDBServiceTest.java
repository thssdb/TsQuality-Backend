package cn.edu.tsinghua.tsquality.services;

import cn.edu.tsinghua.tsquality.service.IoTDBService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBServiceTest {
  @Autowired IoTDBService service;

  @Test
  void getLatestNumericTimeSeriesPath() {
    String path = service.getLatestNumericTimeSeriesPath();
    System.out.println(path);
  }

  @Test
  void getLatestTimeSeriesData() {
    System.out.println(service.getTimeSeriesData("", 10));
  }
}
