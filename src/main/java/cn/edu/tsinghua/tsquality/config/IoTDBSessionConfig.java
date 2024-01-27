package cn.edu.tsinghua.tsquality.config;

import lombok.Data;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "iotdb")
public class IoTDBSessionConfig {
  private String host = "localhost";
  private int port = 6667;
  private String username = "root";
  private String password = "root";
  private int maxSize = 100;

  @Bean
  SessionPool iotdbSessionPool() {
    return new SessionPool(host, port, username, password, maxSize);
  }
}
