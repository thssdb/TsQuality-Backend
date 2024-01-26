package cn.edu.tsinghua.tsquality.generators;

import java.util.Random;

public class TimestampGenerator {
  public static final long START_TIMESTAMP = 1600_000_000_000L;
  public static final long INTERVAL = 1_000L;

  public static long[] timestampsWithHalfAnomalies(int size) {
    long[] timestamps = standardTimestamps(size);
    for (int i = 0; i < size; i++) {
      if (i % 2 == 0) {
        timestamps[i] += abnormalInterval();
      }
    }
    return timestamps;
  }

  public static long[] standardTimestamps(int size) {
    return standardTimestamps(size, INTERVAL);
  }

  public static long[] standardTimestamps(int size, long interval) {
    long[] timestamps = new long[size];
    for (int i = 0; i < size; i++) {
      timestamps[i] = START_TIMESTAMP + i * interval;
    }
    return timestamps;
  }

  private static long abnormalInterval() {
    return abnormalInterval(INTERVAL);
  }

  private static long abnormalInterval(long interval) {
    long result;
    Random random = new Random();
    do {
      result = random.nextLong() % (2 * interval) - interval;
    } while (result == 0 || result == -interval);
    return result;
  }
}
