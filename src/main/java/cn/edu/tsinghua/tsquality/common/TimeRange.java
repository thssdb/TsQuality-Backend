package cn.edu.tsinghua.tsquality.common;

import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@Getter
public class TimeRange implements Comparable<TimeRange> {
  private long min = 0L;
  private long max = 0L;
  @Setter private boolean leftClose = true;
  @Setter private boolean rightClose = true;

  public static String getTimeFilter(List<TimeRange> ranges) {
    return String.join(" and ", ranges.stream().map(TimeRange::getTimeFilter).toList());
  }

  public static String getStatsTimeFilter(
      List<TimeRange> ranges, String minTimestampName, String maxTimestampName) {
    return String.join(" and ",
        ranges.stream().map(x -> x.getStatsTimeFilter(minTimestampName, maxTimestampName)).toList());
  }

  public static List<TimeRange> getRemains(List<TimeRange> lhs, List<TimeRange> rhs) {
    List<TimeRange> result = new ArrayList<>();
    for (TimeRange timeRange : lhs) {
      result.addAll(timeRange.getRemains(rhs));
    }
    return TimeRange.sortAndMerge(result);
  }

  public String getTimeFilter() {
    String left = "time " + (leftClose ? ">= " : "> ") + min;
    String right = "time " + (rightClose ? "<= " : "< ") + max;
    return String.format("(%s and %s)", left, right);
  }

  public String getStatsTimeFilter(String minTimestampName, String maxTimestampName) {
    String left = minTimestampName + (leftClose ? ">= " : "> ") + min;
    String right = maxTimestampName + (rightClose ? "<= " : "< ") + max;
    return String.format("(%s and %s)", left, right);
  }

  public TimeRange(long min, long max) {
    this.set(min, max);
  }

  public int compareTo(@NotNull TimeRange r) {
    if (this.min > r.min) {
      return 1;
    } else if (this.min < r.min) {
      return -1;
    } else if (this.max > r.max) {
      return 1;
    } else {
      return this.max < r.max ? -1 : 0;
    }
  }

  public void setMin(long min) {
    if (min >= 0L && min <= this.max) {
      this.min = min;
    } else {
      throw new IllegalArgumentException("Invalid input!");
    }
  }

  public void setMax(long max) {
    if (max >= 0L && max >= this.min) {
      this.max = max;
    } else {
      throw new IllegalArgumentException("Invalid input!");
    }
  }

  public boolean contains(TimeRange r) {
    return this.min <= r.min && this.max >= r.max;
  }

  public boolean contains(long time) {
    return contains(time, time);
  }

  public boolean contains(long min, long max) {
    if (this.leftClose && this.rightClose) {
      return this.min <= min && this.max >= max;
    } else if (this.leftClose) {
      return this.min <= min && this.max > max;
    } else if (this.rightClose) {
      return this.min < min && this.max >= max;
    } else {
      return this.min < min && this.max > max;
    }
  }

  public void set(long min, long max) {
    if (min > max) {
      throw new IllegalArgumentException("min:" + min + " should not be larger than max: " + max);
    } else {
      this.min = min;
      this.max = max;
    }
  }

  public boolean intersects(TimeRange r) {
    if ((!this.leftClose || !r.rightClose) && r.max < this.min) {
      return false;
    } else if (!this.leftClose && !r.rightClose && r.max <= this.min) {
      return false;
    } else if (this.leftClose && r.rightClose && r.max <= this.min - 2L) {
      return false;
    } else if ((!this.rightClose || !r.leftClose) && r.min > this.max) {
      return false;
    } else if (!this.rightClose && !r.leftClose && r.min >= this.max) {
      return false;
    } else {
      return !this.rightClose || !r.leftClose || r.min < this.max + 2L;
    }
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      TimeRange that = (TimeRange) o;
      return this.min == that.min && this.max == that.max;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(this.min, this.max);
  }

  public boolean overlaps(TimeRange rhs) {
    if ((!this.leftClose || !rhs.rightClose) && rhs.max <= this.min) {
      return false;
    } else if (!this.leftClose && !rhs.rightClose && rhs.max <= this.min + 1L) {
      return false;
    } else if (this.leftClose && rhs.rightClose && rhs.max < this.min) {
      return false;
    } else if ((!this.rightClose || !rhs.leftClose) && rhs.min >= this.max) {
      return false;
    } else if (!this.rightClose && !rhs.leftClose && rhs.min + 1L >= this.max) {
      return false;
    } else {
      return !this.rightClose || !rhs.leftClose || rhs.min <= this.max;
    }
  }

  public String toString() {
    StringBuilder res = new StringBuilder();
    if (this.leftClose) {
      res.append("[ ");
    } else {
      res.append("( ");
    }

    res.append(this.min).append(" : ").append(this.max);
    if (this.rightClose) {
      res.append(" ]");
    } else {
      res.append(" )");
    }

    return res.toString();
  }

  public static List<TimeRange> sortAndMerge(List<TimeRange> unionCandidates) {
    Collections.sort(unionCandidates);
    ArrayList<TimeRange> unionResult = new ArrayList<>();
    Iterator<TimeRange> iterator = unionCandidates.iterator();
    if (iterator.hasNext()) {
      TimeRange rangeCurr = iterator.next();

      while (iterator.hasNext()) {
        TimeRange rangeNext = iterator.next();
        if (rangeCurr.intersects(rangeNext)) {
          rangeCurr.merge(rangeNext);
        } else {
          unionResult.add(rangeCurr);
          rangeCurr = rangeNext;
        }
      }
      unionResult.add(rangeCurr);
    }
    return unionResult;
  }

  public void merge(TimeRange rhs) {
    this.set(Math.min(this.getMin(), rhs.getMin()), Math.max(this.getMax(), rhs.getMax()));
  }

  public List<TimeRange> getRemains(List<TimeRange> timeRangesPrev) {
    List<TimeRange> remains = new ArrayList<>();
    for (TimeRange prev : timeRangesPrev) {
      if (prev.min >= this.max + 2L) {
        break;
      }

      if (this.intersects(prev)) {
        if (prev.contains(this)) {
          return remains;
        }

        if (this.contains(prev)) {
          if (prev.min > this.min && prev.max == this.max) {
            this.setMax(prev.min);
            this.setRightClose(false);
            remains.add(this);
            return remains;
          }
          if (prev.min != this.min) {
            TimeRange r = new TimeRange(this.min, prev.min);
            r.setLeftClose(this.leftClose);
            r.setRightClose(false);
            remains.add(r);
          }
        } else {
          if (prev.min >= this.min) {
            this.setMax(prev.min);
            this.setRightClose(false);
            remains.add(this);
            return remains;
          }
        }
        this.min = prev.max;
        this.leftClose = false;
      }
    }
    remains.add(this);
    return remains;
  }
}
