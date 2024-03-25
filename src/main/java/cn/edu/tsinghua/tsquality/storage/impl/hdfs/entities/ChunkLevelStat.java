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

  public static String[] columnsOrder() {
    return new String[]{
      "path", "filePath", "offset", "minTime", "maxTime", "count", "missCount", "specialCount", "lateCount", "redundancyCount", "valueCount", "variationCount", "speedCount", "accelerationCount"
    };
  }

  public ChunkLevelStat(String path, TsFileInfo info, IoTDBSeriesStat stat, long offset) {
    super(path, info, stat);
    this.offset = offset;
    this.filePath = info.getFilePath();
  }
}
