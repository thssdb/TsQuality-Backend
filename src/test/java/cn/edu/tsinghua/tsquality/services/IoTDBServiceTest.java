package cn.edu.tsinghua.tsquality.services;

import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import cn.edu.tsinghua.tsquality.service.IoTDBService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBServiceTest {
  @Autowired IoTDBService service;

  @Autowired IoTDBConfigMapper mapper;

  @Test
  void getLatestNumericTimeSeriesPath()
      throws IoTDBConnectionException, StatementExecutionException {
    IoTDBConfig config = mapper.getWithPasswordById(1);
    try (Session session = IoTDBService.buildSession(config)) {
      assert session != null;
      String path = service.getLatestNumericTimeSeriesPath(session);
      System.out.println(path);
    }
  }

  @Test
  void getLatestTimeSeriesData() {
    System.out.println(service.getTimeSeriesData(1, "", 10));
  }
}
