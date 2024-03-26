package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ChunkLevelStat extends MetadataStat {
  private long offset;

  public ChunkLevelStat(long version, String path, IoTDBSeriesStat stat, long offset) {
    super(version, path, stat);
    this.offset = offset;
  }
}
