package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkSeriesStatMapper {
  void createChunkSeriesStatTable();

  void insert(@Param("cid") int cid, @Param("sid") int sid, @Param("stat") IoTDBSeriesStat stat);

  List<IoTDBSeriesStat> selectByPath(String path);

  IoTDBSeriesStat selectAsStatByPath(String path);

  IoTDBSeriesStat selectStats(
      @Param("path") String path, @Param("timeRanges") List<TimeRange> timeRanges);

  List<TimeRange> selectTimeRanges(
      @Param("path") String path, @Param("timeRanges") List<TimeRange> timeRanges);
}
