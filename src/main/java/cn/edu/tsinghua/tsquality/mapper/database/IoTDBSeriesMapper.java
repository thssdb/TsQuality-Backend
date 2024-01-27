package cn.edu.tsinghua.tsquality.mapper.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface IoTDBSeriesMapper {
  void createSeriesTable();

  void insertList(@Param("list") List<IoTDBSeries> seriesList);

  @Select("SELECT sid FROM series WHERE path = #{path}")
  int selectIdByPath(@Param("path") String path);
}
