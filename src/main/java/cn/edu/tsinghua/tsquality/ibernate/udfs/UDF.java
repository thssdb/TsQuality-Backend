package cn.edu.tsinghua.tsquality.ibernate.udfs;

import lombok.Getter;

@Getter
public enum UDF {
  TIMESTAMP_REPAIR("timestamprepair"),
  VALUE_REPAIR("valuerepair");

  private final String name;

  UDF(String name) {
    this.name = name;
  }
}
