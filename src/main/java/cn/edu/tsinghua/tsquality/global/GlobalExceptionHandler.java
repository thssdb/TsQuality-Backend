package cn.edu.tsinghua.tsquality.global;

import cn.edu.tsinghua.tsquality.model.dto.ResponseDto;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Log4j2
@RestControllerAdvice
public class GlobalExceptionHandler {
  @ExceptionHandler(Exception.class)
  public ResponseDto<String> handleException(Exception e) {
    log.error(e);
    e.printStackTrace();
    return ResponseDto.fail(ResultCode.FAIL, e.getMessage());
  }
}
