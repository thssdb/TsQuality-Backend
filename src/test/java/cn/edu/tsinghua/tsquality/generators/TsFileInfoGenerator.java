package cn.edu.tsinghua.tsquality.generators;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileInfo;
import org.springframework.stereotype.Component;

@Component
public class TsFileInfoGenerator {
  public TsFileInfo tsFileInfo() {
    return TsFileInfo.builder()
        .database(IoTDBDataGenerator.DATABASE_NAME)
        .filePath("test.tsfile")
        .fileVersion(1000)
        .build();
  }
}
