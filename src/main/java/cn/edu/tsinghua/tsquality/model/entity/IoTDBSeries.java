package cn.edu.tsinghua.tsquality.model.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IoTDBSeries {

  public static List<IoTDBSeries> fromPaths(List<Path> paths, String database) {
    return paths.stream().map(x -> fromPath(x, database)).toList();
  }

  public static IoTDBSeries fromPath(Path path, String database) {
    return IoTDBSeries.builder()
        .path(path.getFullPath())
        .device(path.getDevice())
        .database(database)
        .build();
  }

  private int sid;
  private String path;
  private String device;
  private String database;

  public IoTDBSeries(String path) {
    this.path = path;
    this.device = "";
    this.database = "";
  }
}
