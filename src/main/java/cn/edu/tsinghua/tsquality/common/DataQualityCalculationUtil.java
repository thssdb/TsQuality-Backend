package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.storage.DQType;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

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

  public static List<Double> statToDQMetrics(IoTDBSeriesStat stat, List<DQType> dqTypes) {
    List<Double> result = new ArrayList<>();
    for (DQType type : dqTypes) {
      switch (type) {
        case COMPLETENESS:
          result.add(calculateCompleteness(stat));
          break;
        case CONSISTENCY:
          result.add(calculateConsistency(stat));
          break;
        case TIMELINESS:
          result.add(calculateTimeliness(stat));
          break;
        case VALIDITY:
          result.add(calculateValidity(stat));
          break;
      }
    }
    return result;
  }
}
