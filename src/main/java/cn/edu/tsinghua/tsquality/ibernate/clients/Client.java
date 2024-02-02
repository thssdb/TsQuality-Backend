package cn.edu.tsinghua.tsquality.ibernate.clients;

public interface Client {
  long countTimeSeries();

  long countDevices();

  long countDatabases();
}
