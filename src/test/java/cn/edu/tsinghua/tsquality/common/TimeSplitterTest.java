package cn.edu.tsinghua.tsquality.common;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.common.time.TimePeriodType;
import cn.edu.tsinghua.tsquality.common.time.TimeSplitter;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

import java.util.List;

class TimeSplitterTest {
  @Test
  void testSplitIntoYears() {
    long startTimestamp = 1589904000000L; // 2020-05-20 00:00:00 UTC
    long endTimestamp = 1711900800000L;  // 2024-04-01 00:00:00 UTC

    List<TimeRange> result = TimeSplitter.split(new TimeRange(startTimestamp, endTimestamp), TimePeriodType.YEAR);

    assertThat(result).hasSize(5);
    assertThat(result.getFirst().equals(new TimeRange(1589904000000L, 1609459199000L))).isTrue();
    assertThat(result.get(1).equals(new TimeRange(1609459200000L, 1640995199000L))).isTrue();
    assertThat(result.get(2).equals(new TimeRange(1640995200000L, 1672531199000L))).isTrue();
    assertThat(result.get(3).equals(new TimeRange(1672531200000L, 1704067199000L))).isTrue();
    assertThat(result.getLast().equals(new TimeRange(1704067200000L, 1711900800000L))).isTrue();
  }

  @Test
  void testSplitIntoMonths() {
    long startTimestamp = 1589904000000L; // 2020-05-20 00:00:00 UTC
    long endTimestamp = 1594425600000L;  // 2020-07-11 00:00:00 UTC

    List<TimeRange> result = TimeSplitter.split(new TimeRange(startTimestamp, endTimestamp), TimePeriodType.MONTH);

    assertThat(result).hasSize(3);
    assertThat(result.getFirst().equals(new TimeRange(1589904000000L, 1590969599000L))).isTrue();
    assertThat(result.get(1).equals(new TimeRange(1590969600000L, 1593561599000L))).isTrue();
    assertThat(result.getLast().equals(new TimeRange(1593561600000L, 1594425600000L))).isTrue();
  }
}