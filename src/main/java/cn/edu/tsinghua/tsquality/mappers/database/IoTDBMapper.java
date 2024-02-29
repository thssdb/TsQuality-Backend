package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class IoTDBMapper {
  private final MetadataStorageEngine storageEngine;

  public IoTDBMapper(MetadataStorageEngine storageEngine) {
    this.storageEngine = storageEngine;
  }

  public List<IoTDBSeriesStat> selectSeriesStat() {
    return storageEngine.selectSeriesStats(null);
  }

  public List<IoTDBSeriesStat> selectDeviceStat(String path) {
    return storageEngine.selectDeviceStats(path);
  }

  public List<IoTDBSeriesStat> selectDatabaseStat(String path) {
    return storageEngine.selectDatabaseStats(path);
  }

  public IoTDBSeriesStat selectAllStat() {
    return storageEngine.selectAllStats();
  }
}
