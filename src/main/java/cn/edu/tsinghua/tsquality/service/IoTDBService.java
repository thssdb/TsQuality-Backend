package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.common.IoTDBUtil;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionRequest;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionResult;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesRecentDataDto;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBTimeValuePair;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class IoTDBService {
  private static final String SQL_QUERY_SHOW_TIME_SERIES = "SHOW TIMESERIES";

  private final SessionPool sessionPool;

  public IoTDBService(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  public IoTDBSeriesAnomalyDetectionResult getAnomalyDetectionResult(
      IoTDBSeriesAnomalyDetectionRequest request) {
    IoTDBSeriesAnomalyDetectionResult result = new IoTDBSeriesAnomalyDetectionResult(request);
    SessionDataSetWrapper wrapper = null;
    try {
      String sql = IoTDBUtil.constructQuerySQL(request.getSeriesPath(), request);
      wrapper = sessionPool.executeQueryStatement(sql);
      if (wrapper.getColumnNames().size() != 2) {
        return result;
      }
      SessionDataSet.DataIterator iterator = wrapper.iterator();
      List<IoTDBTimeValuePair> timeValuePairs =
          IoTDBTimeValuePair.buildFromDatasetIterator(iterator);
      result.anomalyDetect(timeValuePairs);
      return result;
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return result;
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  // get the full path of the latest time series with type == DOUBLE | FLOAT | INT32 | INT64
  public String getLatestNumericTimeSeriesPath() {
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(SQL_QUERY_SHOW_TIME_SERIES);
      SessionDataSet.DataIterator iterator = wrapper.iterator();
      while (iterator.next()) {
        if (IoTDBUtil.isNumericDataType(iterator.getString("DataType"))) {
          return iterator.getString("Timeseries");
        }
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
    return "";
  }

  public TimeSeriesRecentDataDto getTimeSeriesData(String path, long limit) {
    try {
      if (path == null || path.isEmpty()) {
        path = getLatestNumericTimeSeriesPath();
      }
      if (path.isEmpty()) {
        return new TimeSeriesRecentDataDto();
      }
      return IoTDBUtil.query(sessionPool, path, limit);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new TimeSeriesRecentDataDto();
    }
  }

  public List<String> getLatestTimeSeriesPath(String path, int limit) {
    try {
      return IoTDBUtil.showLatestTimeSeries(sessionPool, path, limit);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new ArrayList<>();
    }
  }
}
