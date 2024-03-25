package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PageLevelStat extends MetadataStat {
  private String filePath;
  private String index;

  public PageLevelStat(String path, TsFileInfo info, IoTDBSeriesStat stat, String index) {
    super(path, stat);
    this.index = index;
    this.filePath = info.getFilePath();
  }
}
