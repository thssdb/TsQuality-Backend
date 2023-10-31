package cn.edu.tsinghua.tsquality.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBSeriesMapper {
    void createSeriesTable(@Param("tableName") String tableName);
}
