package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.TimestampAnomalyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/timestamp-anomaly")
public class TimestampAnomalyController {
  private final TimestampAnomalyService service;

  public TimestampAnomalyController(TimestampAnomalyService service) {
    this.service = service;
  }

  @GetMapping
  public TimestampAnomalyResultDto anomalyDetectionAndRepair(@RequestParam("path") String path) {
    return service.anomalyDetectionAndRepair(path);
  }
}
