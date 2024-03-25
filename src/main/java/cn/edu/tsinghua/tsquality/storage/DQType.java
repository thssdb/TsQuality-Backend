package cn.edu.tsinghua.tsquality.storage;

public enum DQType {
  COMPLETENESS("completeness"),
  CONSISTENCY("consistency"),
  TIMELINESS("timeliness"),
  VALIDITY("validity");

  public static DQType fromValue(String value) {
    for (DQType type : DQType.values()) {
      if (type.value.equalsIgnoreCase(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid dq type: " + value);
  }

  private final String value;

  DQType(String value) {
    this.value = value;
  }
}
