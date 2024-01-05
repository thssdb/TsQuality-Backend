package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import org.jetbrains.annotations.NotNull;

public class DataQualityCalculationUtil {
  public static double calculateCompleteness(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getMissCnt() / (stat.getCnt() + stat.getMissCnt());
  }

  public static double calculateConsistency(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getRedundancyCnt() / stat.getCnt();
  }

  public static double calculateTimeliness(@NotNull IoTDBSeriesStat stat) {
    return 1 - (double) stat.getLateCnt() / stat.getCnt();
  }

  public static double calculateValidity(@NotNull IoTDBSeriesStat stat) {
    return 1
        - (double)
                (stat.getValueCnt()
                    + stat.getVariationCnt()
                    + stat.getSpecialCnt()
                    + stat.getAccelerationCnt())
            / (4 * stat.getCnt());
  }
}
