package cn.edu.tsinghua.tsquality.generators;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.preaggregation.datastructures.TsFileStat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Component;

@Component
public class SeriesStatGenerator {
  public static final String DATABASE = IoTDBDataGenerator.DATABASE_NAME;
  public static final List<Path> PATHS = IoTDBDataGenerator.paths;
  public static final int SERIES_PER_FILE = 10;
  public static final int CHUNKS_PER_FILE = 10;
  public static final int PAGES_PER_CHUNK = 10;
  public static final int NUM_FILES = 1;
  public static final int NUM_CHUNKS = NUM_FILES * CHUNKS_PER_FILE;
  public static final int NUM_PAGES = NUM_CHUNKS * PAGES_PER_CHUNK;
  public static final int CNT_PER_PAGE = 10;
  public static final int CNT_PER_CHUNK = CNT_PER_PAGE * PAGES_PER_CHUNK;
  public static final int CNT_PER_FILE = CNT_PER_CHUNK * CHUNKS_PER_FILE;
  public static final long MIN_TIMESTAMP = 1000;
  public static final long TIME_INTERVAL = 1;
  public static final long MIN_CHUNK_OFFSET = 0;
  public static final long CHUNK_OFFSET_INTERVAL = 1000;

  public IoTDBSeriesStat seriesStat(long cnt, long minTimestamp, long maxTimestamp) {
    IoTDBSeriesStat stat = new IoTDBSeriesStat();
    stat.setCount(cnt);
    stat.setMinTime(minTimestamp);
    stat.setMaxTime(maxTimestamp);
    return stat;
  }

  public Map<Path, TsFileStat> tsFileStats() {
    Map<Path, TsFileStat> stats = new HashMap<>();
    for (Path path : PATHS) {
      stats.put(path, tsFileStat(path));
    }
    return stats;
  }

  public TsFileStat tsFileStat(Path path) {
    TsFileStat stat = new TsFileStat(path, TSDataType.INT32);
    stat.setFileStat(fileStat());
    stat.setChunkStats(chunkStats());
    stat.setPageStats(pageStats());
    return stat;
  }

  public IoTDBSeriesStat fileStat() {
    return seriesStat(CNT_PER_FILE, MIN_TIMESTAMP, MIN_TIMESTAMP + CNT_PER_FILE * TIME_INTERVAL);
  }

  public Map<Long, IoTDBSeriesStat> chunkStats() {
    Map<Long, IoTDBSeriesStat> stats = new HashMap<>();
    for (int i = 0; i < NUM_CHUNKS; i++) {
      long offset = MIN_CHUNK_OFFSET + i * CHUNK_OFFSET_INTERVAL;
      long minTimestamp = MIN_TIMESTAMP + i * PAGES_PER_CHUNK * CNT_PER_PAGE * TIME_INTERVAL;
      long maxTimestamp = minTimestamp + PAGES_PER_CHUNK * CNT_PER_PAGE * TIME_INTERVAL;
      IoTDBSeriesStat stat = seriesStat(CNT_PER_CHUNK, minTimestamp, maxTimestamp);
      stats.put(offset, stat);
    }
    return stats;
  }

  public Map<Long, List<IoTDBSeriesStat>> pageStats() {
    HashMap<Long, List<IoTDBSeriesStat>> stats = new HashMap<>();
    for (int i = 0; i < NUM_CHUNKS; i++) {
      long chunkOffset = MIN_CHUNK_OFFSET + i * CHUNK_OFFSET_INTERVAL;
      List<IoTDBSeriesStat> pageStats = new ArrayList<>();
      long chunkMinTimestamp = MIN_TIMESTAMP + i * PAGES_PER_CHUNK * CNT_PER_PAGE * TIME_INTERVAL;
      for (int j = 0; j < PAGES_PER_CHUNK; j++) {
        long minTimestamp = chunkMinTimestamp + j * CNT_PER_PAGE * TIME_INTERVAL;
        long maxTimestamp = minTimestamp + CNT_PER_PAGE * TIME_INTERVAL;
        IoTDBSeriesStat stat = seriesStat(CNT_PER_PAGE, minTimestamp, maxTimestamp);
        pageStats.add(stat);
      }
      stats.put(chunkOffset, pageStats);
    }
    return stats;
  }
}
