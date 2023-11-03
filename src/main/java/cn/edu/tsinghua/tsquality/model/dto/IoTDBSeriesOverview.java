package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;

@Data
public class IoTDBSeriesOverview {
    private int cnt;
    private double completeness;
    private double consistency;
    private double timeliness;
    private double validity;
    private String path;

    public IoTDBSeriesOverview(IoTDBSeriesStat stat) {
        this.cnt = stat.getCnt();
        this.completeness = calculateCompleteness(stat);
        this.consistency = calculateConsistency(stat);
        this.timeliness = calculateTimeliness(stat);
        this.validity = calculateValidity(stat);
        this.path = stat.getPath();
    }

    private static double calculateCompleteness(IoTDBSeriesStat stat) {
        return 1 - (double) stat.getMissCnt() / (stat.getCnt() + stat.getMissCnt());
    }

    private static double calculateConsistency(IoTDBSeriesStat stat) {
        return 1 - (double) stat.getRedundancyCnt() / stat.getCnt();
    }

    public static double calculateTimeliness(IoTDBSeriesStat stat) {
        return 1 - (double) stat.getLateCnt() / stat.getCnt();
    }

    public static double calculateValidity(IoTDBSeriesStat stat) {
        return 1 - (double) (
                stat.getValueCnt() + stat.getVariationCnt() + stat.getSpecialCnt() + stat.getAccelerationCnt()
        ) / (4 * stat.getCnt());
    }
}
