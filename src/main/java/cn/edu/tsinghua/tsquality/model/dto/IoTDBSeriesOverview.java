package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class IoTDBSeriesOverview {
  private long cnt;
  private double completeness;
  private double consistency;
  private double timeliness;
  private double validity;
  private String path;

  public IoTDBSeriesOverview(@NotNull IoTDBSeriesStat stat) {
    this.cnt = stat.getCount();
    this.completeness = DataQualityCalculationUtil.calculateCompleteness(stat);
    this.consistency = DataQualityCalculationUtil.calculateConsistency(stat);
    this.timeliness = DataQualityCalculationUtil.calculateTimeliness(stat);
    this.validity = DataQualityCalculationUtil.calculateValidity(stat);
    this.path = stat.getPath();
  }
}
