package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.model.entity.SeriesStatWithTime;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DataQualityMapper {

  long selectSeriesCount();

  long selectDevicesCount();

  long selectDatabasesCount();

  List<IoTDBSeriesStat> selectSeriesStat(@Param("limit") int limit, @Param("offset") int offset);

  List<IoTDBSeriesStat> selectDeviceStat(@Param("limit") int limit, @Param("offset") int offset);

  List<IoTDBSeriesStat> selectDatabaseStat(@Param("limit") int limit, @Param("offset") int offset);

  IoTDBSeriesStat selectAllStat();

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromFileStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromChunkStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);

  List<SeriesStatWithTime> getDataQualityAggregationDetailFromPageStats(
      @Param("path") String path, @Param("aggregationType") String aggregationType);
}
