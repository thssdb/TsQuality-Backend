package cn.edu.tsinghua.tsquality.exceptions;

public class InvalidParametersException extends RuntimeException {
  public InvalidParametersException(String message) {
    super(message);
  }

  public InvalidParametersException(String message, Throwable cause) {
    super(message, cause);
  }
}
