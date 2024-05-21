package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.*;
import cn.edu.tsinghua.tsquality.model.dto.timeseries.TimeSeriesRecentDataDto;
import cn.edu.tsinghua.tsquality.service.IoTDBService;
import cn.edu.tsinghua.tsquality.service.dataprofile.DataProfileService;
import java.util.List;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("api/v1/iotdb")
public class IoTDBController {
  private final IoTDBService iotdbService;
  private final DataProfileService dataProfileService;

  public IoTDBController(IoTDBService iotdbService, DataProfileService dataProfileService) {
    this.iotdbService = iotdbService;
    this.dataProfileService = dataProfileService;
  }

  @GetMapping("/time-series/count")
  public long getNumsTimeSeries() {
    return dataProfileService.getNumTimeSeries();
  }

  @GetMapping("/devices/count")
  public long getNumsDevices() {
    return dataProfileService.getNumDevices();
  }

  @GetMapping("/databases/count")
  public long getNumsDatabases() {
    return dataProfileService.getNumDatabases();
  }

  @GetMapping("/overall-data-profile")
  public IoTDBDataProfile getOverallDataProfile() {
    return dataProfileService.getOverallDataProfile();
  }

  @GetMapping("/time-series/latest")
  public List<String> getLatestTimeSeriesPaths(
      @RequestParam(name = "path", required = false, defaultValue = "root") String path,
      @RequestParam(name = "limit", required = false, defaultValue = "10") int limit) {
    return iotdbService.getLatestTimeSeriesPath(path, limit);
  }

  @GetMapping("/time-series/overview")
  public OverviewResponseDto getTimeSeriesOverview(
      @RequestParam("pageIndex") int pageIndex, @RequestParam("pageSize") int pageSize) {
    return dataProfileService.getTimeSeriesOverview(pageIndex, pageSize);
  }

  @PostMapping("/time-series/anomaly-detection")
  public IoTDBSeriesAnomalyDetectionResult getAnomalyDetectionResult(
      @RequestBody(required = false) IoTDBSeriesAnomalyDetectionRequest request) {
    return iotdbService.getAnomalyDetectionResult(request);
  }

  @GetMapping("/devices/overview")
  public OverviewResponseDto getDeviceOverview(
      @RequestParam("pageIndex") int pageIndex, @RequestParam("pageSize") int pageSize) {
    return dataProfileService.getDeviceOverview(pageIndex, pageSize);
  }

  @GetMapping("/databases/overview")
  public OverviewResponseDto getDatabaseOverview(
      @RequestParam("pageIndex") int pageIndex, @RequestParam("pageSize") int pageSize) {
    return dataProfileService.getDatabaseOverview(pageIndex, pageSize);
  }

  @GetMapping("/time-series/data")
  public TimeSeriesRecentDataDto getTimeSeriesData(
      @RequestParam(required = false, defaultValue = "") String path,
      @RequestParam(required = false, defaultValue = "100") Long limit) {
    return iotdbService.getTimeSeriesData(path, limit);
  }
}
