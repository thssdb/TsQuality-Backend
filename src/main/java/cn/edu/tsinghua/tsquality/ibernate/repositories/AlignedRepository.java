package cn.edu.tsinghua.tsquality.ibernate.repositories;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public interface AlignedRepository {
  void createAlignedTimeSeries(List<TSDataType> dataTypes)
      throws IoTDBConnectionException, StatementExecutionException;

  long count() throws IoTDBConnectionException, StatementExecutionException;

  long countTimeSeriesLike(String prefix)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteAlignedTimeSeries() throws IoTDBConnectionException, StatementExecutionException;

  void insert(long timestamp, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insert(long timestamp, List<String> measurements, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  List<List<Object>> select(String timeFilter, String valueFilter)
      throws IoTDBConnectionException, StatementExecutionException;
}
