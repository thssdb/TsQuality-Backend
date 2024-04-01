package cn.edu.tsinghua.tsquality.common.time;

public enum TimePeriodType {
  YEAR("year"),
  MONTH("month"),
  DAY("day"),
  HOUR("hour"),
  MINUTE("minute");

  public final String value;

  public static TimePeriodType fromValue(String value) {
    for (TimePeriodType type : TimePeriodType.values()) {
      if (type.value.equalsIgnoreCase(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid split type: " + value);
  }

  TimePeriodType(String value) {
    this.value = value;
  }
}
