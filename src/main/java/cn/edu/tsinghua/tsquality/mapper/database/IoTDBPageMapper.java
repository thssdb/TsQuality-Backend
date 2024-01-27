package cn.edu.tsinghua.tsquality.mapper.database;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface IoTDBPageMapper {
  void createPageTable();
}
