package cn.edu.tsinghua.tsquality.generators;

import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import org.springframework.stereotype.Component;

@Component
public class TsFileInfoGenerator {
  public TsFileInfo tsFileInfo() {
    return TsFileInfo.builder()
        .database("root.database1")
        .filePath("test.tsfile")
        .fileVersion(1000)
        .build();
  }
}
