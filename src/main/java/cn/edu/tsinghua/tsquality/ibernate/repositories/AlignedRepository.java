package cn.edu.tsinghua.tsquality.ibernate.repositories;

import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public interface AlignedRepository {
  void createAlignedTimeSeries(List<TSDataType> dataTypes)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteAlignedTimeSeries() throws IoTDBConnectionException, StatementExecutionException;

  void insert(long timestamp, List<TSDataType> dataTypes, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  Object select(List<Path> first, String timeFilter, String valueFilter)
      throws IoTDBConnectionException, StatementExecutionException;
}
