package cn.edu.tsinghua.tsquality.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "pre-aggregation")
public class PreAggregationConfig {
  public TableNames tables = new TableNames();

  @Data
  public static class TableNames {
    public String series = "series";
    public String file = "file";
    public String chunk = "chunk";
    public String page = "page";
    public String fileSeriesStat = "file_series_stat";
    public String chunkSeriesStat = "chunk_series_stat";
    public String pageSeriesStat = "page_series_stat";
  }
}
