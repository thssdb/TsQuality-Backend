package cn.edu.tsinghua.tsquality.mapper;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBChunkMapperTest {
  @Autowired PreAggregationConfig config;
  @Autowired IoTDBChunkMapper mapper;

  @Test
  void insert() {
    IoTDBChunk chunk = new IoTDBChunk(1, 1, 100);
    mapper.insert(config.tables.chunk, chunk);
    System.out.println(chunk.cid);
  }
}
