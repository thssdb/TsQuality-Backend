package cn.edu.tsinghua.tsquality.model.dto.timeseries;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class TimeSeriesOverviewDto {
  private long minTimestamp;
  private long maxTimestamp;
  private long count;
}
