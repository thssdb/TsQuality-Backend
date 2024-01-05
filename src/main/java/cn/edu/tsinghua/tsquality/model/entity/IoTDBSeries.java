package cn.edu.tsinghua.tsquality.model.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IoTDBSeries {
  private int sid;
  private String path;
  private String device = "";
  private String database = "";

  public IoTDBSeries(String path) {
    this.path = path;
  }

  public IoTDBSeries(String path, String device) {
    this.path = path;
    this.device = device;
  }
}
