package cn.edu.tsinghua.tsquality.global;

import lombok.Getter;

@Getter
public enum ResultCode {
  SUCCESS(0, "success"),
  FAIL(1, "fail")
  ;

  private final int code;

  private final String message;

  ResultCode(int code, String message) {
    this.code = code;
    this.message = message;
  }
}
