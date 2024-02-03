package cn.edu.tsinghua.tsquality.ibernate.repositories;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public interface AlignedRepository {
  void createAlignedTimeSeries(List<TSDataType> dataTypes) throws IoTDBConnectionException, StatementExecutionException;

  void deleteAlignedTimeSeries() throws IoTDBConnectionException, StatementExecutionException;
}
