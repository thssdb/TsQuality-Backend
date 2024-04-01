package cn.edu.tsinghua.tsquality.common.time;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class TimeSplitter {
  public static List<TimeRange> split(TimeRange timeRange, TimePeriodType splitType) {
    List<TimeRange> result = new ArrayList<>();

    long currentTimestamp = timeRange.getMin();
    LocalDateTime currentDateTime = LocalDateTime.ofEpochSecond(currentTimestamp / 1000, 0, ZoneOffset.UTC);
    LocalDateTime endDateTime = LocalDateTime.ofEpochSecond(timeRange.getMax() / 1000, 0, ZoneOffset.UTC);

    while (currentDateTime.isBefore(endDateTime)) {
      LocalDateTime nextDateTime = switch (splitType) {
        case YEAR -> currentDateTime.plusYears(1).withDayOfYear(1).withHour(0).withMinute(0).withSecond(0);
        case MONTH -> currentDateTime.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0);
        case DAY -> currentDateTime.plusDays(1).withHour(0).withMinute(0).withSecond(0);
        case HOUR -> currentDateTime.plusHours(1).withMinute(0).withSecond(0);
        case MINUTE -> currentDateTime.plusMinutes(1).withSecond(0);
      };
      if (nextDateTime.isAfter(endDateTime)) {
        nextDateTime = endDateTime;
      }
      long next = nextDateTime.toEpochSecond(ZoneOffset.UTC) * 1000;
      result.add(new TimeRange(currentTimestamp, next == timeRange.getMax() ? next : next - 1000));
      currentTimestamp = next;
      currentDateTime = nextDateTime;
    }
    return result;
  }

}
