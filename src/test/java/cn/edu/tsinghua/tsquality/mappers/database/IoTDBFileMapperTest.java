package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBFileMapperTest {
  @Autowired private IoTDBFileMapper ioTDBFileMapper;

  @Test
  void insert() {
    IoTDBFile file = new IoTDBFile("test3.tsfile", 1);
    ioTDBFileMapper.insert(file);
    System.out.println(file.getFid());
  }

  @Test
  void select() {
    Map<String, Object> res = ioTDBFileMapper.select();
    System.out.println(res);
  }
}
