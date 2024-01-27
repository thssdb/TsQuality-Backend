package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.global.ResultCode;
import lombok.Data;

@Data
public class ResponseDto<T> {
  private int code;
  private T data;
  private String message;

  public static <T> ResponseDto<T> success(T data) {
    ResponseDto<T> response = new ResponseDto<>();
    response.setCode(0);
    response.setData(data);
    response.setMessage("success");
    return response;
  }

  public static <T> ResponseDto<T> fail(int code, String message) {
    ResponseDto<T> response = new ResponseDto<>();
    response.setCode(code);
    response.setMessage(message);
    return response;
  }

  public static <T> ResponseDto<T> fail(ResultCode code) {
    return fail(code.getCode(), code.getMessage());
  }

  public static <T> ResponseDto<T> fail(ResultCode code, String message) {
    return fail(code.getCode(), message);
  }
}
