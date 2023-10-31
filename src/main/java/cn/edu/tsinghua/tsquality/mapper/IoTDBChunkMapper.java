package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkMapper {
    void createChunkTable(
            @Param("chunkTableName") String chunkTableName,
            @Param("fileTableName") String fileTableName
    );
}
