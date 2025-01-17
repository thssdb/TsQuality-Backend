package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.udfs.AbstractUDF;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public interface Repository {
  void createTimeSeries(TSDataType dataType);

  void deleteTimeSeries();

  long count(String timeFilter);

  long selectMinTimestamp();

  long selectMaxTimestamp();

  TVList select(AbstractUDF udf, String timeFilter, String valueFilter);

  TVList select(long limit);

  TVList select(String timeFilter, String valueFilter);

  void insert(TVList tvList);

  void flush() throws IoTDBConnectionException, StatementExecutionException;
}
