package cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.request;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.AnomalyRequestDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class TimestampAnomalyRequestDto extends AnomalyRequestDto {
  private String method;
  private Long interval;
}
