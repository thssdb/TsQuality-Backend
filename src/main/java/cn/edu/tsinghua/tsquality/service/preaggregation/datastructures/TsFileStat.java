package cn.edu.tsinghua.tsquality.service.preaggregation.datastructures;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import lombok.Getter;
import lombok.Setter;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileStat {
  private final Path seriesPath;
  @Getter
  @Setter
  private IoTDBSeriesStat fileStat = new IoTDBSeriesStat();
  @Getter
  @Setter
  private Map<Long, IoTDBSeriesStat> chunkStats = new HashMap<>();
  @Getter
  @Setter
  private Map<Long, List<IoTDBSeriesStat>> pageStats = new HashMap<>();

  public TsFileStat(Path path) {
    seriesPath = path;
  }

  public void addPageSeriesStat(long chunkOffset, IoTDBSeriesStat stat) {
    chunkStats.get(chunkOffset).merge(stat);
    pageStats.get(chunkOffset).add(stat);
  }

  public void startNewChunk(long chunkOffset) {
    chunkStats.put(chunkOffset, new IoTDBSeriesStat());
    pageStats.put(chunkOffset, new ArrayList<>());
  }

  public void endChunk(long chunkOffset) {
    fileStat.merge(chunkStats.get(chunkOffset));
  }
}
