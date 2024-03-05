package cn.edu.tsinghua.tsquality.storage;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;
import java.util.Map;

public interface MetadataStorageEngine {
  void saveTsFileStats(TsFileInfo tsFileInfo, Map<Path, TsFileStat> stats);

  List<IoTDBSeriesStat> selectSeriesStats(String path);

  List<IoTDBSeriesStat> selectDeviceStats(String path);

  List<IoTDBSeriesStat> selectDatabaseStats(String path);

  IoTDBSeriesStat selectAllStats();

  List<Double> getDataQuality(List<DQType> dqTypes, String path, List<TimeRange> timeRanges);
}
