package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.timeseries.impl.TimeSeriesDataQualityServiceImpl;
import cn.edu.tsinghua.tsquality.storage.DQType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

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
      @RequestParam("types") List<DQType> dqTypes,
      @RequestParam("path") String path,
      @RequestParam("startTime") Long startTime,
      @RequestParam("endTime") Long endTime) {
    List<TimeRange> timeRanges = List.of(new TimeRange(startTime, endTime));
    return service.getTimeSeriesDQMetrics(dqTypes, path, timeRanges);
  }
}
