package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.response.TimestampAnomalyResponseDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.request.TimestampAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.service.timeseries.TimestampAnomalyService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/timestamp-anomaly")
public class TimestampAnomalyController {
  private final TimestampAnomalyService service;

  public TimestampAnomalyController(TimestampAnomalyService service) {
    this.service = service;
  }

  @PostMapping
  public TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      TimestampAnomalyRequestDto dto) {
    return service.anomalyDetectionAndRepair(dto);
  }
}
