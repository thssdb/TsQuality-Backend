package cn.edu.tsinghua.tsquality.common.datastructures;

import lombok.Getter;

@Getter
public class TimeRange extends org.apache.iotdb.tsfile.read.common.TimeRange {
  public TimeRange(long min, long max) {
    super(min, max);
  }
}
