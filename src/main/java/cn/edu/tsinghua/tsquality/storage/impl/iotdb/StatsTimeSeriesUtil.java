package cn.edu.tsinghua.tsquality.storage.impl.iotdb;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

public class StatsTimeSeriesUtil {
  public static final List<String> STATS_PATHS;
  public static final List<TSDataType> STATS_DATA_TYPES;
  public static final String FILE_STATS_PATH_PREFIX = "file_stats";
  public static final String FILE_PATH_MEASUREMENT = "path";
  public static final String FILE_VERSION_MEASUREMENT = "file_version";
  public static final String CHUNK_STATS_PATH_PREFIX = "chunk_stats";
  public static final String CHUNK_OFFSET_MEASUREMENT = "offset";
  public static final String PAGE_STATS_PATH_PREFIX = "page_stats";
  public static final String PAGE_INDEX_MEASUREMENT = "index";

  public static final String VERSION = "version";
  public static final String MIN_TIME = "min_time";
  public static final String MAX_TIME = "max_time";
  public static final String MIN_VALUE = "min_value";
  public static final String MAX_VALUE = "max_value";
  public static final String COUNT = "count";
  public static final String MISS_COUNT = "miss_count";
  public static final String SPECIAL_COUNT = "special_count";
  public static final String LATE_COUNT = "late_count";
  public static final String REDUNDANCY_COUNT = "redundancy_count";
  public static final String VALUE_COUNT = "value_count";
  public static final String VARIATION_COUNT = "variation_count";
  public static final String SPEED_COUNT = "speed_count";
  public static final String ACCELERATION_COUNT = "acceleration_count";

  static {
    STATS_PATHS =
        List.of(
            VERSION,
            MIN_TIME,
            MAX_TIME,
            MIN_VALUE,
            MAX_VALUE,
            COUNT,
            MISS_COUNT,
            SPECIAL_COUNT,
            LATE_COUNT,
            REDUNDANCY_COUNT,
            VALUE_COUNT,
            VARIATION_COUNT,
            SPEED_COUNT,
            ACCELERATION_COUNT);
    STATS_DATA_TYPES =
        List.of(
            TSDataType.TEXT,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64,
            TSDataType.INT64);
  }

  public static List<Object> getValuesForStat(IoTDBSeriesStat stat) {
    return List.of(
        stat.getMinTime(),
        stat.getMaxTime(),
        stat.getMinValue(),
        stat.getMaxValue(),
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

  public static String getFileStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), FILE_STATS_PATH_PREFIX);
  }

  public static List<String> getFileStatsMeasurementsForPath() {
    List<String> measurements = new ArrayList<>(List.of(FILE_PATH_MEASUREMENT, FILE_VERSION_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }

  public static String getChunkStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), CHUNK_STATS_PATH_PREFIX);
  }

  public static List<String> getChunkStatsMeasurementsForPath() {
    List<String> measurements = new ArrayList<>(List.of(CHUNK_OFFSET_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }

  public static String getPageStatsDeviceForPath(Path path) {
    return String.format("%s.%s", path.getFullPath(), PAGE_STATS_PATH_PREFIX);
  }

  public static List<String> getPageStatsMeasurementsForPath() {
    List<String> measurements = new ArrayList<>(List.of(PAGE_INDEX_MEASUREMENT));
    measurements.addAll(STATS_PATHS);
    return measurements;
  }
}
