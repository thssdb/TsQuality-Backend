package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkSeriesStatMapper {
  void createChunkSeriesStatTable(
      @Param("chunkTableName") String chunkTableName,
      @Param("chunkSeriesStatTableName") String chunkSeriesStatTableName);

  void insert(
      @Param("tableName") String tableName,
      @Param("cid") int cid,
      @Param("stat") IoTDBSeriesStat stat);
}
