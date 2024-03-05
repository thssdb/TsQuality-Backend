package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface IoTDBChunkSeriesStatMapper {
  void createChunkSeriesStatTable();

  void insert(@Param("cid") int cid, @Param("stat") IoTDBSeriesStat stat);

  IoTDBSeriesStat selectStats(String path, List<TimeRange> timeRanges);

  List<TimeRange> selectTimeRanges(String path, List<TimeRange> timeRanges);
}
