package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBFileSeriesStatMapper {
    void createFileSeriesStatTable(
            @Param("fileTableName") String fileTableName,
            @Param("seriesTableName") String seriesTableName,
            @Param("fileSeriesStatTableName") String fileSeriesStatTableName
    );
}
