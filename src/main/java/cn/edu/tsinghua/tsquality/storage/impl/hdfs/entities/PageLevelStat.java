package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PageLevelStat extends MetadataStat {
  private String index;

  public PageLevelStat(long version, String path, IoTDBSeriesStat stat, String index) {
    super(version, path, stat);
    this.index = index;
  }
}
