package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.*;
import cn.edu.tsinghua.tsquality.service.IoTDBService;
import java.util.List;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/v1/iotdb")
public class IoTDBController {
  private final IoTDBService iotdbService;

  public IoTDBController(IoTDBService iotdbService) {
    this.iotdbService = iotdbService;
  }

  @GetMapping("/time-series/count")
  public long getNumsTimeSeries() {
    return iotdbService.getNumsTimeSeries();
  }

  @GetMapping("/devices/count")
  public long getNumsDevices() {
    return iotdbService.getNumsDevices();
  }

  @GetMapping("/databases/count")
  public long getNumsDatabases() {
    return iotdbService.getNumsDatabases();
  }

  @GetMapping("/storage-groups/count")
  public long getNumsStorageGroups() {
    return iotdbService.getNumsStorageGroups();
  }

  @GetMapping("/aggregation-info")
  public IoTDBAggregationInfoDto getAggregationInfo() {
    return iotdbService.getAggregationInfo();
  }

  @GetMapping("/time-series/latest")
  public List<String> getLatestTimeSeriesPaths(
      @RequestParam(name = "path", required = false, defaultValue = "root") String path,
      @RequestParam(name = "limit", required = false, defaultValue = "10") int limit) {
    return iotdbService.getLatestTimeSeriesPath(path, limit);
  }

  @GetMapping("/time-series/overview")
  public List<IoTDBSeriesOverview> getTimeSeriesOverview() {
    return iotdbService.getTimeSeriesOverview();
  }

  @PostMapping("/time-series/anomaly-detection")
  public IoTDBSeriesAnomalyDetectionResult getAnomalyDetectionResult(
      @RequestBody(required = false) IoTDBSeriesAnomalyDetectionRequest request) {
    return iotdbService.getAnomalyDetectionResult(request);
  }

  @GetMapping("/devices/overview")
  public List<IoTDBSeriesOverview> getDeviceOverview(
      @RequestParam(value = "path", required = false) String path) {
    return iotdbService.getDeviceOverview(path);
  }

  @GetMapping("/databases/overview")
  public List<IoTDBSeriesOverview> getDatabaseOverview(
      @RequestParam(value = "path", required = false) String path) {
    return iotdbService.getDatabaseOverview(path);
  }

  @GetMapping("/time-series/data")
  public TimeSeriesRecentDataDto getTimeSeriesData(
      @RequestParam(required = false, defaultValue = "") String path,
      @RequestParam(required = false, defaultValue = "10") Long limit) {
    return iotdbService.getTimeSeriesData(path, limit);
  }
}
