package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkMapper {
  void createChunkTable();

  void insert(@Param("chunk") IoTDBChunk chunk);

  List<IoTDBChunk> select();

  List<IoTDBChunk> selectByPath(@Param("path") String path);
}
