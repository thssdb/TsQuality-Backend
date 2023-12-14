package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface DataQualityMapper {

    List<IoTDBSeriesStat> selectSeriesStat(
            @Param("seriesTableName") String seriesTableName,
            @Param("fileSeriesStatTableName") String fileSeriesStatTableName,
            @Param("path") String path);

    List<IoTDBSeriesStat> selectDeviceStat(
            @Param("seriesTableName") String seriesTableName,
            @Param("fileSeriesStatTableName") String fileSeriesStatTableName,
            @Param("path") String path);

    List<IoTDBSeriesStat> selectDatabaseStat(
            @Param("seriesTableName") String seriesTableName,
            @Param("fileSeriesStatTableName") String fileSeriesStatTableName,
            @Param("path") String path);

    IoTDBSeriesStat selectAllStat(@Param("fileSeriesStatTableName") String fileSeriesStatTableName);
}
