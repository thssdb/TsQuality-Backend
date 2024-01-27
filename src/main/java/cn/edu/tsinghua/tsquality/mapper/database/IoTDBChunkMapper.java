package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkMapper {
  void createChunkTable();

  void insert(@Param("chunk") IoTDBChunk chunk);
}
