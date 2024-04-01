package cn.edu.tsinghua.tsquality.common.time;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimeFormatter {
  public static String timestampToTimePeriodString(long timestamp, TimePeriodType type) {
    LocalDateTime time = LocalDateTime.ofEpochSecond(timestamp / 1000, 0, ZoneOffset.ofHours(8));
    return switch (type) {
      case YEAR -> time.format(DateTimeFormatter.ofPattern("yyyy"));
      case MONTH -> time.format(DateTimeFormatter.ofPattern("yyyy-MM"));
      case DAY -> time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
      case HOUR -> time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH"));
      case MINUTE -> time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
    };
  }
}
