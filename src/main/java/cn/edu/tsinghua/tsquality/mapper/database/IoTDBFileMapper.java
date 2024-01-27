package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import org.apache.ibatis.annotations.*;

import java.util.HashMap;
import java.util.Map;

@Mapper
public interface IoTDBFileMapper {
  void createFileTable();

  @Options(useGeneratedKeys = true, keyProperty = "file.fid")
  @Insert(
      "INSERT IGNORE INTO files (file_version, file_path) "
          + "VALUES (#{file.fileVersion}, #{file.filePath})")
  int insert(@Param("file") IoTDBFile file);

  @ResultType(HashMap.class)
  @Select("SELECT * FROM files LIMIT 1")
  Map<String, Object> select();

  int selectIdByFilePath(@Param("filePath") String filePath);
}
