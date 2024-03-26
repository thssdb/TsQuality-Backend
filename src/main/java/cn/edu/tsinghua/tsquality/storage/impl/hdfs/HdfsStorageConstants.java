package cn.edu.tsinghua.tsquality.storage.impl.hdfs;

public class HdfsStorageConstants {
  public static final String METADATA_DIRNAME = "iotdb_dq_metadata/";
  public static final String FILES_DIRNAME = METADATA_DIRNAME + "files";
  public static final String FILE_SERIES_STATS_DIRNAME = METADATA_DIRNAME + "file_series_stats";
  public static final String CHUNK_SERIES_STATS_DIRNAME = METADATA_DIRNAME + "chunk_series_stats";
  public static final String PAGE_SERIES_STATS_DIRNAME = METADATA_DIRNAME + "page_series_stats";
}
