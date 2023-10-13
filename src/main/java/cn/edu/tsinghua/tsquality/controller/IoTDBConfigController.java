package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBConfig;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IoTDBConfigController {
  @PostMapping(value = "api/v1/iotdb-config")
  public IoTDBConfig create() {
    return
  }
}
