package cn.edu.tsinghua.tsquality.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IoTDBDataProfile {
  private Long numDataPoints;
  private Long numTimeSeries;
  private Long numDevices;
  private Long numDatabases;
  private Double completeness;
  private Double consistency;
  private Double timeliness;
  private Double validity;
}
