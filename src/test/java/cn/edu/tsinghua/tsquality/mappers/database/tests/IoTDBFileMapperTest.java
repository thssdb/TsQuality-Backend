package cn.edu.tsinghua.tsquality.mappers.database.tests;

import cn.edu.tsinghua.tsquality.mappers.database.IoTDBFileMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class IoTDBFileMapperTest {
  @Autowired private IoTDBFileMapper underTests;

  @Test
  void insert() {
    IoTDBFile file = new IoTDBFile("test3.tsfile", 1);
    underTests.insert(file);
    System.out.println(file.getFid());
  }

  @Test
  void select() {
    Map<String, Object> res = underTests.select();
    System.out.println(res);
  }
}
