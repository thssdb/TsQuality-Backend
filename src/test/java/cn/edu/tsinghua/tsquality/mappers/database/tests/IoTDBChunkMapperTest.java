package cn.edu.tsinghua.tsquality.mappers.database.tests;

import cn.edu.tsinghua.tsquality.mappers.database.IoTDBChunkMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class IoTDBChunkMapperTest {
  @Autowired IoTDBChunkMapper underTests;

  @Test
  void insert() {
    IoTDBChunk chunk = new IoTDBChunk(1, 1, 100);
    underTests.insert(chunk);
    System.out.println(chunk.cid);
  }
}
