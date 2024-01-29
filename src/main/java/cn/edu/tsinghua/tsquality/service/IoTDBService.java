package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.common.IoTDBUtil;
import cn.edu.tsinghua.tsquality.mappers.database.IoTDBMapper;
import cn.edu.tsinghua.tsquality.model.dto.*;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
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
  private static final String SQL_QUERY_NUMS_TIME_SERIES = "COUNT TIMESERIES";
  private static final String SQL_QUERY_NUMS_DEVICES = "COUNT DEVICES";
  private static final String SQL_QUERY_NUMS_DATABASES = "COUNT DATABASES";
  private static final String SQL_QUERY_SHOW_TIME_SERIES = "SHOW TIMESERIES";

  private final IoTDBMapper iotdbMapper;

  private final SessionPool sessionPool;

  public IoTDBService(IoTDBMapper iotdbMapper, SessionPool sessionPool) {
    this.iotdbMapper = iotdbMapper;
    this.sessionPool = sessionPool;
  }

  public long getCountResult(String sql) {
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      SessionDataSet.DataIterator iterator = wrapper.iterator();
      if (iterator.next()) {
        return iterator.getLong(1);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
    return 0;
  }

  public long getNumsTimeSeries() {
    return getCountResult(SQL_QUERY_NUMS_TIME_SERIES);
  }

  public long getNumsDevices() {
    return getCountResult(SQL_QUERY_NUMS_DEVICES);
  }

  public long getNumsDatabases() {
    return getCountResult(SQL_QUERY_NUMS_DATABASES);
  }

  public long getNumsStorageGroups() {
    return getCountResult(SQL_QUERY_NUMS_DATABASES);
  }

  public List<IoTDBSeriesOverview> getTimeSeriesOverview() {
    return iotdbMapper.selectSeriesStat().stream().map(IoTDBSeriesOverview::new).toList();
  }

  public List<IoTDBSeriesOverview> getDeviceOverview(String path) {
    return iotdbMapper.selectDeviceStat(path).stream().map(IoTDBSeriesOverview::new).toList();
  }

  public List<IoTDBSeriesOverview> getDatabaseOverview(String path) {
    return iotdbMapper.selectDatabaseStat(path).stream().map(IoTDBSeriesOverview::new).toList();
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

  public IoTDBAggregationInfoDto getAggregationInfo() {
    IoTDBSeriesStat stat = iotdbMapper.selectAllStat();
    double completeness = DataQualityCalculationUtil.calculateCompleteness(stat);
    double consistency = DataQualityCalculationUtil.calculateConsistency(stat);
    double timeliness = DataQualityCalculationUtil.calculateTimeliness(stat);
    double validity = DataQualityCalculationUtil.calculateValidity(stat);
    return IoTDBAggregationInfoDto.builder()
        .numDataPoints(stat.getCnt())
        .numTimeSeries(getNumsTimeSeries())
        .numDevices(getNumsDevices())
        .numDatabases(getNumsDatabases())
        .completeness(completeness)
        .consistency(consistency)
        .timeliness(timeliness)
        .validity(validity)
        .build();
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
