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

  public List<IoTDBSeriesStat> selectSeriesStat(int pageIndex, int pageSize) {
    return storageEngine.selectSeriesStats(pageIndex, pageSize);
  }

  public List<IoTDBSeriesStat> selectDeviceStat(int pageIndex, int pageSize) {
    return storageEngine.selectDeviceStats(pageIndex, pageSize);
  }

  public List<IoTDBSeriesStat> selectDatabaseStat(int pageIndex, int pageSize) {
    return storageEngine.selectDatabaseStats(pageIndex, pageSize);
  }

  public IoTDBSeriesStat selectAllStat() {
    return storageEngine.selectAllStats();
  }
}
