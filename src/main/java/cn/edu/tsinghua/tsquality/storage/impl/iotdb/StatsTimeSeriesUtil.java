package cn.edu.tsinghua.tsquality.storage.impl.iotdb;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Component;

@Component
public class StatsTimeSeriesUtil {
  public static final List<String> STATS_PATHS;
  public static final List<TSDataType> STATS_DATA_TYPES;
  public static final String FILE_STATS_PATH_PREFIX = "file_stats";
  public static final String FILE_PATH_MEASUREMENT = "path";
  public static final String CHUNK_STATS_PATH_PREFIX = "chunk_stats";
  public static final String CHUNK_OFFSET_MEASUREMENT = "offset";
  public static final String PAGE_STATS_PATH_PREFIX = "page_stats";
  public static final String PAGE_INDEX_MEASUREMENT = "index";

  static {
    STATS_PATHS =
        List.of(
            "min_timestamp",
            "max_timestamp",
            "count",
            "miss_count",
            "special_count",
            "late_count",
            "redundancy_count",
            "value_count",
            "variation_count",
            "speed_count",
            "acceleration_count");
    STATS_DATA_TYPES = Collections.nCopies(STATS_PATHS.size(), TSDataType.INT64);
  }

  public List<Object> getValuesForStat(IoTDBSeriesStat stat) {
    return List.of(
        stat.getMinTimestamp(),
        stat.getMaxTimestamp(),
        stat.getCount(),
        stat.getMissCount(),
        stat.getSpecialCount(),
        stat.getLateCount(),
        stat.getRedundancyCount(),
        stat.getValueCount(),
        stat.getVariationCount(),
        stat.getSpecialCount(),
        stat.getAccelerationCount());
  }

  public String getFileStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), FILE_STATS_PATH_PREFIX);
  }

  public List<String> getFileStatsMeasurementsForPath(Path path) {
    List<String> measurements = new ArrayList<>(List.of(FILE_PATH_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }

  public List<TSDataType> getFileStatsDataTypesForPath(Path path) {
    List<TSDataType> dataTypes = new ArrayList<>(List.of(TSDataType.TEXT));
    dataTypes.addAll(STATS_DATA_TYPES);
    return dataTypes;
  }

  public String getChunkStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), CHUNK_STATS_PATH_PREFIX);
  }

  public List<String> getChunkStatsMeasurementsForPath(Path path) {
    List<String> measurements = new ArrayList<>(List.of(CHUNK_OFFSET_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }

  public List<TSDataType> getChunkStatsDataTypesForPath(Path path) {
    List<TSDataType> dataTypes = new ArrayList<>(List.of(TSDataType.INT64));
    dataTypes.addAll(STATS_DATA_TYPES);
    return dataTypes;
  }

  public String getPageStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), PAGE_STATS_PATH_PREFIX);
  }

  public List<String> getPageStatsMeasurementsForPath(Path path) {
    List<String> measurements = new ArrayList<>(List.of(PAGE_INDEX_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }

  public List<TSDataType> getPageStatsDataTypesForPath(Path path) {
    List<TSDataType> dataTypes = new ArrayList<>(List.of(TSDataType.INT32));
    dataTypes.addAll(STATS_DATA_TYPES);
    return dataTypes;
  }
}
