package cn.edu.tsinghua.tsquality.model.dto;

import java.util.List;
import lombok.Data;

@Data
public class OverviewResponseDto {
  long totalCount;
  List<IoTDBSeriesOverview> stats;

  public OverviewResponseDto(long totalCount, List<IoTDBSeriesOverview> stats) {
    this.totalCount = totalCount;
    this.stats = stats;
  }
}
