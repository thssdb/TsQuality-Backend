package cn.edu.tsinghua.tsquality.storage;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.read.common.Path;

public interface MetadataStorageEngine {
  void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats);

  List<IoTDBSeriesStat> selectSeriesStats(String path);

  List<IoTDBSeriesStat> selectDeviceStats(String path);

  List<IoTDBSeriesStat> selectDatabaseStats(String path);

  IoTDBSeriesStat selectAllStats();
}
