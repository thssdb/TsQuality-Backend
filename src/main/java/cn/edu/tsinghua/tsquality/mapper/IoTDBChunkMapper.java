package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkMapper {
    void createChunkTable(
            @Param("chunkTableName") String chunkTableName,
            @Param("fileTableName") String fileTableName);

    void insert(@Param("tableName") String tableName, @Param("chunk") IoTDBChunk chunk);
}
