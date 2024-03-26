package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MetadataStat {
  protected long version;
  protected String path;
  protected long minTime;
  protected long maxTime;
  protected double minValue;
  protected double maxValue;
  protected long count;
  protected long missCount;
  protected long specialCount;
  protected long lateCount;
  protected long redundancyCount;
  protected long valueCount;
  protected long variationCount;
  protected long speedCount;
  protected long accelerationCount;

  public static String[] statSumColumns() {
    return new String[] {
        "min(minTime) as minTime",
        "max(maxTime) as maxTime",
        "sum(count) as count",
        "sum(missCount) as missCount",
        "sum(specialCount) as specialCount",
        "sum(lateCount) as lateCount",
        "sum(redundancyCount) as redundancyCount",
        "sum(valueCount) as valueCount",
        "sum(variationCount) as variationCount",
        "sum(speedCount) as speedCount",
        "sum(accelerationCount) as accelerationCount"
    };
  }

  public MetadataStat(long version, String path, IoTDBSeriesStat stat) {
    this.version = version;
    this.path = path;
    this.minTime = stat.getMinTime();
    this.maxTime = stat.getMaxTime();
    this.count = stat.getCount();
    this.missCount = stat.getMissCount();
    this.specialCount = stat.getSpecialCount();
    this.lateCount = stat.getLateCount();
    this.redundancyCount = stat.getRedundancyCount();
    this.valueCount = stat.getValueCount();
    this.variationCount = stat.getVariationCount();
    this.specialCount = stat.getSpeedCount();
    this.accelerationCount = stat.getAccelerationCount();
  }
}
