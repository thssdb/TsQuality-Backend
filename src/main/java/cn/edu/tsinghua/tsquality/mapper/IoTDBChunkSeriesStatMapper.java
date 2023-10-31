package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkSeriesStatMapper {
    void createChunkSeriesStatTable(
            @Param("chunkTableName") String chunkTableName,
            @Param("chunkSeriesStatTableName") String chunkSeriesStatTableName
    );
}
