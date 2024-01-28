package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.model.entity.SeriesStatWithTime;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface DataQualityMapper {

  List<IoTDBSeriesStat> selectSeriesStat(@Param("path") String path);

  List<IoTDBSeriesStat> selectDeviceStat(@Param("path") String path);

  List<IoTDBSeriesStat> selectDatabaseStat(@Param("path") String path);

  IoTDBSeriesStat selectAllStat();

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromFileStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromChunkStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromPageStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);
}
