package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.udfs.AbstractUDF;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public interface Repository {
  void createTimeSeries(TSDataType dataType);

  void deleteTimeSeries();

  TVList select(AbstractUDF udf, String timeFilter, String valueFilter);

  TVList select(String timeFilter, String valueFilter);

  void insert(TVList tvList);
}
