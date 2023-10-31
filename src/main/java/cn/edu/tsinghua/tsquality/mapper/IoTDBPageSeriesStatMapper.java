package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBPageSeriesStatMapper {
    void createPageSeriesStatTable(
            @Param("pageTableName") String pageTableName,
            @Param("pageSeriesStatTableName") String pageSeriesStatTableName
    );
}
