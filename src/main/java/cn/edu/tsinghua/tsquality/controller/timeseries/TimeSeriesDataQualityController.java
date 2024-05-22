package cn.edu.tsinghua.tsquality.controller.timeseries;

import cn.edu.tsinghua.tsquality.common.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.service.timeseries.impl.TimeSeriesDataQualityServiceImpl;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@RestController
@RequestMapping("api/v1/time-series/data-quality")
public class TimeSeriesDataQualityController {
  private final TimeSeriesDataQualityServiceImpl service;

  public TimeSeriesDataQualityController(TimeSeriesDataQualityServiceImpl service) {
    this.service = service;
  }

  @GetMapping("/aggregation")
  public LinkedHashMap<String, List<Double>> getTimeSeriesDQAggregationDetail(
      @RequestParam("path") String path,
      @RequestParam("type") String type,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime) {
    return service.getAggregateDQMetrics(type, path, startTime, endTime);
  }

  @GetMapping("/data-size")
  public LinkedHashMap<String, Long> getDataSizeDistribution(
      @RequestParam("path") String path,
      @RequestParam("type") String type,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime) {
    return service.getDataSizeDistribution(type, path, startTime, endTime);
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
