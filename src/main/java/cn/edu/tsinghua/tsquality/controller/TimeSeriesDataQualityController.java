package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.impl.TimeSeriesDataQualityServiceImpl;
import java.util.Collections;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/time-series/data-quality")
public class TimeSeriesDataQualityController {
  private final TimeSeriesDataQualityServiceImpl service;

  public TimeSeriesDataQualityController(TimeSeriesDataQualityServiceImpl service) {
    this.service = service;
  }

  @GetMapping("/aggregation")
  public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
      @RequestParam(value = "path", required = false) String path,
      @RequestParam("type") String type) {
    DQAggregationType aggregationType = DQAggregationType.valueOf(type);
    return service.getTimeSeriesDQAggregationDetail(path, aggregationType, Collections.emptyList());
  }
}
