package cn.edu.tsinghua.tsquality.model.dto.anomalies.value.request;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.AnomalyRequestDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ValueAnomalyRequestDto extends AnomalyRequestDto {
  private String method;
  private String args;
}
