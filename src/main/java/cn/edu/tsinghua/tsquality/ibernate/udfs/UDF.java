package cn.edu.tsinghua.tsquality.ibernate.udfs;

import lombok.Getter;

@Getter
public enum UDF {
  TIMESTAMP_REPAIR("timestamprepair");

  private final String name;

  UDF(String name) {
    this.name = name;
  }
}
