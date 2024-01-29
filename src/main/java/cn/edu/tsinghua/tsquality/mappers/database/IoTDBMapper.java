package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class IoTDBMapper {
  private final DataQualityMapper dataQualityMapper;

  public IoTDBMapper(DataQualityMapper dataQualityMapper) {
    this.dataQualityMapper = dataQualityMapper;
  }

  public List<IoTDBSeriesStat> selectSeriesStat() {
    return dataQualityMapper.selectSeriesStat(null);
  }

  public List<IoTDBSeriesStat> selectDeviceStat(String path) {
    return dataQualityMapper.selectDeviceStat(path);
  }

  public List<IoTDBSeriesStat> selectDatabaseStat(String path) {
    return dataQualityMapper.selectDatabaseStat(path);
  }

  public IoTDBSeriesStat selectAllStat() {
    return dataQualityMapper.selectAllStat();
  }
}
