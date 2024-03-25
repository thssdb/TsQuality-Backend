package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.timeseries.impl.TimeSeriesDataQualityServiceImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @GetMapping("/dq")
  public List<Double> getDataQuality(
      @RequestParam("types") List<String> dqTypes,
      @RequestParam("path") String path,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime) {
    long min = startTime == null ? Long.MIN_VALUE : startTime;
    long max = endTime == null ? Long.MAX_VALUE : endTime;
    List<TimeRange> timeRanges =
        (startTime != null || endTime != null)
            ? List.of(new TimeRange(min, max))
            : new ArrayList<>();
    return service.getTimeSeriesDQMetrics(dqTypes, path, timeRanges);
  }
}
