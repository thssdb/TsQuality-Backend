package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBPageSeriesStatMapper {
  void createPageSeriesStatTable();

  void insert(@Param("pid") int pid, @Param("stat") IoTDBSeriesStat stat);
}
