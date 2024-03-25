package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ChunkLevelStat extends MetadataStat {
  private String filePath;
  private long offset;

  public ChunkLevelStat(String path, TsFileInfo info, IoTDBSeriesStat stat, long offset) {
    super(path, stat);
    this.offset = offset;
    this.filePath = info.getFilePath();
  }
}
