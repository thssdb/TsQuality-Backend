package cn.edu.tsinghua.tsquality.preaggregation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TsFileInfo {
    private String filePath;
    // which database(aka storage group) this tsfile belongs to
    // will be used in /api/v1/iotdb/{id}/database/overview
    private String database;
    private long fileVersion;
}
