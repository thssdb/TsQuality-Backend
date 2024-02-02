package cn.edu.tsinghua.tsquality.service.dataprofile.impl;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBDataProfile;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesOverview;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.dataprofile.DataProfileService;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
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
public class DataProfileServiceImpl implements DataProfileService {
  private static final String SQL_QUERY_NUMS_TIME_SERIES = "COUNT TIMESERIES";
  private static final String SQL_QUERY_NUMS_DEVICES = "COUNT DEVICES";
  private static final String SQL_QUERY_NUMS_DATABASES = "COUNT DATABASES";

  private final MetadataStorageEngine storageEngine;

  private final SessionPool sessionPool;

  public DataProfileServiceImpl(MetadataStorageEngine storageEngine, SessionPool sessionPool) {
    this.storageEngine = storageEngine;
    this.sessionPool = sessionPool;
  }

  @Override
  public Long getNumTimeSeries() {
    return getCountResult(SQL_QUERY_NUMS_TIME_SERIES);
  }

  @Override
  public Long getNumDevices() {
    return getCountResult(SQL_QUERY_NUMS_DEVICES);
  }

  @Override
  public Long getNumDatabases() {
    return getCountResult(SQL_QUERY_NUMS_DATABASES);
  }

  private long getCountResult(String sql) {
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

  @Override
  public IoTDBDataProfile getOverallDataProfile() {
    IoTDBSeriesStat stat = storageEngine.selectAllStats();
    double completeness = DataQualityCalculationUtil.calculateCompleteness(stat);
    double consistency = DataQualityCalculationUtil.calculateConsistency(stat);
    double timeliness = DataQualityCalculationUtil.calculateTimeliness(stat);
    double validity = DataQualityCalculationUtil.calculateValidity(stat);
    return IoTDBDataProfile.builder()
        .numDataPoints(stat.getCnt())
        .numTimeSeries(getNumTimeSeries())
        .numDevices(getNumDevices())
        .numDatabases(getNumDatabases())
        .completeness(completeness)
        .consistency(consistency)
        .timeliness(timeliness)
        .validity(validity)
        .build();
  }

  @Override
  public List<IoTDBSeriesOverview> getTimeSeriesOverview() {
    return storageEngine.selectSeriesStats(null).stream().map(IoTDBSeriesOverview::new).toList();
  }

  @Override
  public List<IoTDBSeriesOverview> getDeviceOverview(String path) {
    return storageEngine.selectDeviceStats(path).stream().map(IoTDBSeriesOverview::new).toList();
  }

  @Override
  public List<IoTDBSeriesOverview> getDatabaseOverview(String path) {
    return storageEngine.selectDatabaseStats(path).stream().map(IoTDBSeriesOverview::new).toList();
  }
}
