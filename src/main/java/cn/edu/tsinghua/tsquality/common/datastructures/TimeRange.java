package cn.edu.tsinghua.tsquality.common.datastructures;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

@Getter
public class TimeRange implements Comparable<TimeRange> {
  private long min = 0L;
  private long max = 0L;
  private boolean isLeftClose = true;
  private boolean isRightClose = true;

  public TimeRange(long min, long max) {
    set(min, max);
  }

  public int compareTo(@NotNull TimeRange range) {
    if (this.min > range.min) {
      return 1;
    } else if (this.min < range.min) {
      return -1;
    } else if (this.max > range.max) {
      return 1;
    }
    return this.max < range.max ? -1 : 0;
  }

  public void set(long min, long max) {
    if (min > max) {
      throw new IllegalArgumentException("min:" + min + " should not be larger than max: " + max);
    } else {
      this.min = min;
      this.max = max;
    }
  }

  public void setMin(long min) {
    if (min < 0L || min > this.max) {
      throw new IllegalArgumentException("Invalid min value");
    }
    this.min = min;
  }

  public void setMax(long max) {
    if (max < 0L || max < this.min) {
      throw new IllegalArgumentException("Invalid max value");
    }
    this.max = max;
  }
}
