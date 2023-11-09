package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class IoTDBSeriesOverview {
    private int cnt;
    private double completeness;
    private double consistency;
    private double timeliness;
    private double validity;
    private String path;
    private String device;
    private String database;

    public IoTDBSeriesOverview(@NotNull IoTDBSeriesStat stat) {
        this.cnt = stat.getCnt();
        this.completeness = calculateCompleteness(stat);
        this.consistency = calculateConsistency(stat);
        this.timeliness = calculateTimeliness(stat);
        this.validity = calculateValidity(stat);
        this.path = stat.getPath();
        this.device = stat.getDevice();
        this.database = stat.getDatabase();
    }

    private static double calculateCompleteness(@NotNull IoTDBSeriesStat stat) {
        return 1 - (double) stat.getMissCnt() / (stat.getCnt() + stat.getMissCnt());
    }

    private static double calculateConsistency(@NotNull IoTDBSeriesStat stat) {
        return 1 - (double) stat.getRedundancyCnt() / stat.getCnt();
    }

    public static double calculateTimeliness(@NotNull IoTDBSeriesStat stat) {
        return 1 - (double) stat.getLateCnt() / stat.getCnt();
    }

    public static double calculateValidity(@NotNull IoTDBSeriesStat stat) {
        return 1 - (double) (
                stat.getValueCnt() + stat.getVariationCnt() + stat.getSpecialCnt() + stat.getAccelerationCnt()
        ) / (4 * stat.getCnt());
    }
}
