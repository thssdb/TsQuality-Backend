package cn.edu.tsinghua.tsquality.mappers.database;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SeriesTestMapper {
  List<String> selectAll();
}
