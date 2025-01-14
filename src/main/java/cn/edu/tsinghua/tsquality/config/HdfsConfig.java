package cn.edu.tsinghua.tsquality.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HdfsConfig {
  @Value("${hdfs.defaultFS:hdfs://localhost:9000/}")
  private String defaultFS;

  @Value("${hdfs.replicas:1}")
  private String replicas;

  @Bean
  public Configuration configuration() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultFS);
    conf.set("dfs.replication", replicas);
    return conf;
  }
}
