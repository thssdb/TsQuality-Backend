package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MetadataStat {
  protected String path;
  protected long minTime;
  protected long maxTime;
  protected long count;
  protected long missCount;
  protected long specialCount;
  protected long lateCount;
  protected long redundancyCount;
  protected long valueCount;
  protected long variationCount;
  protected long speedCount;
  protected long accelerationCount;

  public MetadataStat(String path, TsFileInfo info, IoTDBSeriesStat stat) {
    this.path = path;
    this.minTime = stat.getMinTimestamp();
    this.maxTime = stat.getMaxTimestamp();
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
