package cn.edu.tsinghua.tsquality.ibernate.clients;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.util.List;

public interface Client {
  long countTimeSeries();

  long countDevices();

  long countDatabases();

  List<String> queryLatestTimeSeries(String path, int limit)
      throws IoTDBConnectionException, StatementExecutionException;
}
