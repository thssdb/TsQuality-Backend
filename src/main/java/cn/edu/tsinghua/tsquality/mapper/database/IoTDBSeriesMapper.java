package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface IoTDBSeriesMapper {
  void createSeriesTable(@Param("tableName") String tableName);

  void insertList(
      @Param("tableName") String tableName, @Param("list") List<IoTDBSeries> seriesList);

  @Select("SELECT sid FROM ${tableName} WHERE ts_path = #{path}")
  int selectIdByPath(@Param("tableName") String tableName, @Param("path") String path);
}
