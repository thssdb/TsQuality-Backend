package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;

public interface Repository {
  void createTimeSeries();

  void deleteTimeSeries();

  TVList select(String timeFilter, String valueFilter);

  void insert(TVList tvList);
}
