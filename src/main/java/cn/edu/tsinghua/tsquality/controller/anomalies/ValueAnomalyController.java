package cn.edu.tsinghua.tsquality.controller.anomalies;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.request.ValueAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.response.ValueAnomalyResponseDto;
import cn.edu.tsinghua.tsquality.service.timeseries.ValueAnomalyService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/value-anomaly")
public class ValueAnomalyController {
  private final ValueAnomalyService service;

  public ValueAnomalyController(ValueAnomalyService service) {
    this.service = service;
  }

  @PostMapping
  public ValueAnomalyResponseDto anomalyDetectionAndRepair(
      ValueAnomalyRequestDto dto
  ) {
    return service.anomalyDetectionAndRepair(dto);
  }
}
