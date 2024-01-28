package cn.edu.tsinghua.tsquality.mappers.database;

import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface IoTDBPageMapper {
  void createPageTable();
}
