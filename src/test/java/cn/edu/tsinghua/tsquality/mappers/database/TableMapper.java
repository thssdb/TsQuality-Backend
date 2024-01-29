package cn.edu.tsinghua.tsquality.mappers.database;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface TableMapper {
  List<String> selectAllTables();

  void truncateAllTables();
}
