package cn.edu.tsinghua.tsquality.global;

import cn.edu.tsinghua.tsquality.model.dto.ResponseDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.util.Resource;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

@RestControllerAdvice
public class GlobalResponseAdvice implements ResponseBodyAdvice<Object> {

  private final ObjectMapper objectMapper;

  public GlobalResponseAdvice(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public boolean supports(@NotNull MethodParameter returnType, @NotNull Class converterType) {
    return true;
  }

  @Override
  @SneakyThrows
  public Object beforeBodyWrite(Object body, @NotNull MethodParameter returnType, @NotNull MediaType selectedContentType, @NotNull Class selectedConverterType, ServerHttpRequest request, ServerHttpResponse response) {
    if (body instanceof String) {
      return objectMapper.writeValueAsString(ResponseDto.success(body));
    }
    if (body instanceof ResponseDto || body instanceof byte[] || body instanceof Resource) {
      return body;
    }
    return ResponseDto.success(body);
  }
}
