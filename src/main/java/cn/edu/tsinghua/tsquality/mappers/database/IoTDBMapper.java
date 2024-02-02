package cn.edu.tsinghua.tsquality.mappers.database;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import java.util.List;
import org.springframework.stereotype.Component;

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
