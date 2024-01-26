package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionRequest;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDataPointDto;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesRecentDataDto;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;

public class IoTDBUtil {
  public static boolean isNumericDataType(String dataType) {
    return dataType.equals("INT32")
        || dataType.equals("INT64")
        || dataType.equals("FLOAT")
        || dataType.equals("DOUBLE");
  }

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
    String[] splitRes = splitSeriesPath(seriesPath);
    String device = splitRes[0];
    String sensor = splitRes[1];
    String sql = String.format("SELECT %s FROM %s", sensor, device);
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
      sql += String.format(" %s %s", sensor, valueFilter);
    }
    if (limit != 0) {
      sql += String.format(" LIMIT %d", limit);
    }
    return sql;
  }

  // split time series path to {device}.{sensor}
  public static String[] splitSeriesPath(String seriesPath) {
    String[] result = new String[2];
    int index = seriesPath.lastIndexOf('.');
    if (index == -1) {
      result[0] = seriesPath;
      result[1] = "";
    } else {
      result[0] = seriesPath.substring(0, index);
      result[1] = seriesPath.substring(index + 1);
    }
    return result;
  }

  public static TimeSeriesRecentDataDto query(Session session, String path, long limit)
      throws IoTDBConnectionException, StatementExecutionException, UnSupportedDataTypeException {
    String sql = constructQuerySQL(path, 0, 0, "", limit);
    SessionDataSet.DataIterator iterator = session.executeQueryStatement(sql).iterator();
    List<TimeSeriesDataPointDto> points = new ArrayList<>();
    while (iterator.next()) {
      String dataType = iterator.getColumnTypeList().get(1);
      double value =
          switch (dataType) {
            case "INT32" -> iterator.getInt(2);
            case "INT64" -> iterator.getLong(2);
            case "FLOAT" -> iterator.getFloat(2);
            case "DOUBLE" -> iterator.getDouble(2);
            default -> throw new UnSupportedDataTypeException("Unexpected type: " + dataType);
          };
      TimeSeriesDataPointDto point =
          TimeSeriesDataPointDto.builder().timestamp(iterator.getLong(1)).value(value).build();
      points.add(point);
    }
    return TimeSeriesRecentDataDto.builder().path(path).points(points).build();
  }

  public static List<String> showLatestTimeSeries(Session session, String path, int limit)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = String.format("SHOW LATEST TIMESERIES %s.** LIMIT %d", path, limit);
    SessionDataSet.DataIterator iterator = session.executeQueryStatement(sql).iterator();
    List<String> paths = new ArrayList<>();
    while (iterator.next()) {
      paths.add(iterator.getString(1));
    }
    return paths;
  }
}
