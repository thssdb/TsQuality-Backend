package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.mapper.database.IoTDBChunkMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBChunkMapperTest {
  @Autowired IoTDBChunkMapper mapper;

  @Test
  void insert() {
    IoTDBChunk chunk = new IoTDBChunk(1, 1, 100);
    mapper.insert(chunk);
    System.out.println(chunk.cid);
  }
}
