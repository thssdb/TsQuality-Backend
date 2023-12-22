package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SeriesStatWithTime extends IoTDBSeriesStat {
    // result of querying data quality aggregation detail
    // e.g. query data quality aggregated by day/month/year
    private String time;
}
