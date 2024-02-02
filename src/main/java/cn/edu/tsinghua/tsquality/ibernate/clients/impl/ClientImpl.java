package cn.edu.tsinghua.tsquality.ibernate.clients.impl;

import cn.edu.tsinghua.tsquality.ibernate.clients.Client;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class ClientImpl implements Client {
  private static final String SQL_COUNT_NUM_TIME_SERIES = "count timeseries";
  private static final String SQL_COUNT_NUM_DEVICES = "count devices";
  private static final String SQL_COUNT_NUM_DATABASES = "count databases";

  private final SessionPool sessionPool;

  public ClientImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public long countTimeSeries() {
    return getCountResult(SQL_COUNT_NUM_TIME_SERIES);
  }

  @Override
  public long countDevices() {
    return getCountResult(SQL_COUNT_NUM_DEVICES);
  }

  @Override
  public long countDatabases() {
    return getCountResult(SQL_COUNT_NUM_DATABASES);
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
}
