package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBFileSeriesStatMapper {
  void createFileSeriesStatTable();

  void insert(@Param("fid") int fid, @Param("sid") int sid, @Param("stat") IoTDBSeriesStat stat);
}
