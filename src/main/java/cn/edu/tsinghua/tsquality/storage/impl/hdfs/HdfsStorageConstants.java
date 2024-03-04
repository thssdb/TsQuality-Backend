package cn.edu.tsinghua.tsquality.storage.impl.hdfs;

public class HdfsStorageConstants {
  public static final String metadataDirname = "iotdb_dq_metadata/";
  public static final String seriesFilePath = metadataDirname + "series.csv";
  public static final String filesFilePath = metadataDirname + "files.csv";
  public static final String chunksFilePath = metadataDirname + "chunks.csv";
  public static final String fileSeriesStatsFilePath = metadataDirname + "file_series_stats.csv";
  public static final String chunkSeriesStatsFilePath = metadataDirname + "chunk_series_stats.csv";
}
