package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import java.util.HashMap;
import java.util.Map;
import org.apache.ibatis.annotations.*;

@Mapper
public interface IoTDBFileMapper {
    void createFileTable(@Param("tableName") String tableName);

    @Options(useGeneratedKeys = true, keyProperty = "file.fid")
    @Insert(
            "INSERT IGNORE INTO ${tableName} (file_version, file_path) "
                    + "VALUES (#{file.fileVersion}, #{file.filePath})")
    int insert(@Param("tableName") String tableName, @Param("file") IoTDBFile file);

    @ResultType(HashMap.class)
    @Select("SELECT * FROM ${tableName} LIMIT 1")
    Map<String, Object> select(@Param("tableName") String tableName);

    int selectIdByFilePath(
            @Param("tableName") String tableName, @Param("filePath") String filePath);
}
