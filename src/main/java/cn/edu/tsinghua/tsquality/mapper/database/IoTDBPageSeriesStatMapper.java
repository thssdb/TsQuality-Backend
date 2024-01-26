package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBPageSeriesStatMapper {
  void createPageSeriesStatTable(
      @Param("pageTableName") String pageTableName,
      @Param("pageSeriesStatTableName") String pageSeriesStatTableName);

  void insert(
      @Param("tableName") String tableName,
      @Param("pid") int pid,
      @Param("stat") IoTDBSeriesStat stat);
}
