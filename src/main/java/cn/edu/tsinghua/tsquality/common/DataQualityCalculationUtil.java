package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.jetbrains.annotations.NotNull;

public class DataQualityCalculationUtil {
  public static double calculateCompleteness(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getMissCount() / (stat.getCount() + stat.getMissCount());
  }

  public static double calculateConsistency(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getRedundancyCount() / stat.getCount();
  }

  public static double calculateTimeliness(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getLateCount() / stat.getCount();
  }

  public static double calculateValidity(@NotNull IoTDBSeriesStat stat) {
    return 1
        - (double)
                (stat.getValueCount()
                    + stat.getVariationCount()
                    + stat.getSpecialCount()
                    + stat.getAccelerationCount())
            / (4 * stat.getCount());
  }
}
