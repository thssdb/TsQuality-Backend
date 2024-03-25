package cn.edu.tsinghua.tsquality.storage.impl.hdfs.entities;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FileLevelStat extends MetadataStat {
  private String filePath;
  private long version;

  public static String[] columnsOrder() {
    return new String[]{
      "path", "filePath", "version", "minTime", "maxTime", "count", "missCount", "specialCount", "lateCount", "redundancyCount", "valueCount", "variationCount", "speedCount", "accelerationCount"
    };
  }

  public FileLevelStat(String path, TsFileInfo info, IoTDBSeriesStat stat) {
    super(path, info, stat);
    filePath = info.getFilePath();
    version = info.getFileVersion();
  }
}
