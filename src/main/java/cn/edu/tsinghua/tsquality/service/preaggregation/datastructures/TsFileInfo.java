package cn.edu.tsinghua.tsquality.service.preaggregation.datastructures;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TsFileInfo {
  // which database(aka storage group) this tsfile belongs to
  private String database;
  private String filePath;
  private long fileVersion;

  public TsFileInfo(IoTDBFile file) {
    filePath = file.getFilePath();
    fileVersion = file.getFileVersion();
  }
}
