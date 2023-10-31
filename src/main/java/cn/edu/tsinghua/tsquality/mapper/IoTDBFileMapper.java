package cn.edu.tsinghua.tsquality.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface IoTDBFileMapper {
    void createFileTable(@Param("tableName") String tableName);
}
