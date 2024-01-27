package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkSeriesStatMapper {
  void createChunkSeriesStatTable();

  void insert(@Param("cid") int cid, @Param("stat") IoTDBSeriesStat stat);
}
