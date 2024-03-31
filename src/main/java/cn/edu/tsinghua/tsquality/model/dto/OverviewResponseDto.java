package cn.edu.tsinghua.tsquality.model.dto;

import lombok.Data;

import java.util.List;

@Data
public class OverviewResponseDto {
  long totalCount;
  List<IoTDBSeriesOverview> stats;

  public OverviewResponseDto(long totalCount, List<IoTDBSeriesOverview> stats) {
    this.totalCount = totalCount;
    this.stats = stats;
  }
}
