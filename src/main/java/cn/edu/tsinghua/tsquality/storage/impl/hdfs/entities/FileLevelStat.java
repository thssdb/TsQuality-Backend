package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FileLevelStat extends MetadataStat {
  private String filePath;

  public FileLevelStat(long version, String seriesPath, String filePath, IoTDBSeriesStat stat) {
    super(version, seriesPath, stat);
    this.filePath = filePath;
  }
}
