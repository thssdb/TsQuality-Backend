package cn.edu.tsinghua.tsquality.model.dto.anomalies.value;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class ValueRepairedDataPointDto {
  private Long timestamp;
  private Object original;
  private Object repaired;

  public static ValueRepairedDataPointDto from(TVPair original, TVPair repaired) {
    return ValueRepairedDataPointDto.builder()
        .timestamp(original.getTimestamp())
        .original(original.getValue())
        .repaired(repaired.getValue())
        .build();
  }
}
