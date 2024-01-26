package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TimeSeriesDataPointDto {
  private Long timestamp;
  private Object value;

  public static TimeSeriesDataPointDto from(TVPair pair) {
    return new TimeSeriesDataPointDto(pair.getTimestamp(), pair.getValue());
  }
}
