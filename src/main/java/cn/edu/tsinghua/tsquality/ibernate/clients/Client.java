package cn.edu.tsinghua.tsquality.ibernate.clients;

import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

public interface Client {
  long countTimeSeries();

  long countDevices();

  long countDatabases();

  List<String> queryLatestTimeSeries(String path, int limit)
      throws IoTDBConnectionException, StatementExecutionException;
}
