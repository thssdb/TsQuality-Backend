package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.ValueAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.timeseries.ValueAnomalyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/value-anomaly")
public class ValueAnomalyController {
  private final ValueAnomalyService service;

  public ValueAnomalyController(ValueAnomalyService service) {
    this.service = service;
  }

  @GetMapping
  public ValueAnomalyResultDto anomalyDetectionAndRepair(
      @RequestParam("path") String path, @RequestParam("time") String timeFilter) {
    return service.anomalyDetectionAndRepair(path, timeFilter);
  }
}
