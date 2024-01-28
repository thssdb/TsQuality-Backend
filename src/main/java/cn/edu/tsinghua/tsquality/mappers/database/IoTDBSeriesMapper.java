package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface IoTDBSeriesMapper {
  void createSeriesTable();

  void insertList(@Param("list") List<IoTDBSeries> seriesList);

  @Select("SELECT sid FROM series WHERE path = #{path}")
  int selectIdByPath(@Param("path") String path);
}
