package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBPageMapper {
  void createPageTable(
      @Param("pageTableName") String pageTableName, @Param("chunkTableName") String chunkTableName);
}
