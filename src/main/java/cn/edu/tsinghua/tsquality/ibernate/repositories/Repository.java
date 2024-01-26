package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.udfs.AbstractUDF;

public interface Repository {
  void createTimeSeries();

  void deleteTimeSeries();

  TVList select(AbstractUDF udf, String timeFilter, String valueFilter);

  TVList select(String timeFilter, String valueFilter);

  void insert(TVList tvList);
}
