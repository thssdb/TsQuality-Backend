package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionRequest;
import org.apache.iotdb.tsfile.read.common.Path;

public class IoTDBUtil {
  public static String constructQuerySQL(
      String seriesPath, IoTDBSeriesAnomalyDetectionRequest request) {
    IoTDBSeriesAnomalyDetectionRequest.TimeFilter timeFilter = request.getTimeFilter();
    IoTDBSeriesAnomalyDetectionRequest.ValueFilter valueFilter = request.getValueFilter();
    return constructQuerySQL(
        seriesPath,
        timeFilter == null ? 0 : timeFilter.getMinTimestamp(),
        timeFilter == null ? 0 : timeFilter.getMaxTimestamp(),
        valueFilter == null ? "" : valueFilter.getContent(),
        0);
  }

  public static String constructQuerySQL(
      String seriesPath, long minTimeFilter, long maxTimeFilter, String valueFilter, long limit) {
    Path path = new Path(seriesPath, true);
    String sql = String.format("SELECT %s FROM %s", path.getMeasurement(), path.getDevice());
    if (minTimeFilter != 0) {
      sql += String.format(" WHERE time > %d", minTimeFilter);
    }
    if (maxTimeFilter != 0) {
      if (minTimeFilter != 0) {
        sql += " AND";
      }
      sql += String.format(" time < %d", maxTimeFilter);
    }
    if (valueFilter != null && !valueFilter.isEmpty()) {
      if (minTimeFilter != 0 || maxTimeFilter != 0) {
        sql += " AND";
      }
      sql += String.format(" %s %s", path.getMeasurement(), valueFilter);
    }
    if (limit != 0) {
      sql += String.format(" LIMIT %d", limit);
    }
    return sql;
  }
}
