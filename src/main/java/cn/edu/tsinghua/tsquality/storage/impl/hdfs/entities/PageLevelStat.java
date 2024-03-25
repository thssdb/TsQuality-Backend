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

  public static String[] columnsOrder() {
    return new String[]{
      "path", "filePath", "index", "minTime", "maxTime", "count", "missCount", "specialCount", "lateCount", "redundancyCount", "valueCount", "variationCount", "speedCount", "accelerationCount"
    };
  }

  public PageLevelStat(String path, TsFileInfo info, IoTDBSeriesStat stat, String index) {
    super(path, info, stat);
    this.index = index;
    this.filePath = info.getFilePath();
  }
}
