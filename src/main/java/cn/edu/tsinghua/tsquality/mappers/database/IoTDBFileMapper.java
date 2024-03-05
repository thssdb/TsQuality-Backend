package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBFileMapper {
  void createFileTable();

  @Options(useGeneratedKeys = true, keyProperty = "file.fid")
  @Insert(
      "insert ignore into files(file_version, file_path) "
          + "values (#{file.fileVersion}, #{file.filePath})")
  int insert(@Param("file") IoTDBFile file);

  List<IoTDBFile> select();

  List<IoTDBFile> selectByPath(@Param("path") String path);

  int selectIdByFilePath(@Param("filePath") String filePath);
}
