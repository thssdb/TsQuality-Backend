package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBFileSeriesStatMapper {
    void createFileSeriesStatTable(
            @Param("fileTableName") String fileTableName,
            @Param("seriesTableName") String seriesTableName,
            @Param("fileSeriesStatTableName") String fileSeriesStatTableName
    );

    void insert(
            @Param("tableName") String tableName,
            @Param("fid") int fid,
            @Param("sid") int sid,
            @Param("stat") IoTDBSeriesStat stat);
}
