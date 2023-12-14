package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.*;
import cn.edu.tsinghua.tsquality.service.IoTDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/iotdb")
public class IoTDBController {
    @Autowired
    private IoTDBService iotdbService;

    @GetMapping("/{id}/time-series/count")
    public long getNumsTimeSeries(@PathVariable int id) {
        return iotdbService.getNumsTimeSeries(id);
    }

    @GetMapping("/{id}/devices/count")
    public long getNumsDevices(@PathVariable int id) {
        return iotdbService.getNumsDevices(id);
    }

    @GetMapping("/{id}/databases/count")
    public long getNumsDatabases(@PathVariable int id) {
        return iotdbService.getNumsDatabases(id);
    }

    @GetMapping("/{id}/storage-groups/count")
    public long getNumsStorageGroups(@PathVariable int id) {
        return iotdbService.getNumsStorageGroups(id);
    }

    @GetMapping("/{id}/aggregation-info")
    public IoTDBAggregationInfoDto getAggregationInfo(@PathVariable int id) {
        return iotdbService.getAggregationInfo(id);
    }

    @GetMapping("/{id}/time-series/latest")
    public List<String> getLatestTimeSeriesPaths(
            @PathVariable int id,
            @RequestParam(name = "path", required=false, defaultValue = "root") String path,
            @RequestParam(name = "limit", required=false, defaultValue = "10") int limit
    ) {
        return iotdbService.getLatestTimeSeriesPath(id, path, limit);
    }

    @GetMapping("/{id}/time-series/overview")
    public List<IoTDBSeriesOverview> getTimeSeriesOverview(@PathVariable int id) {
        return iotdbService.getTimeSeriesOverview(id);
    }

    @PostMapping("/{id}/time-series/anomaly-detection")
    public IoTDBSeriesAnomalyDetectionResult getAnomalyDetectionResult(
            @PathVariable int id,
            @RequestBody(required = false) IoTDBSeriesAnomalyDetectionRequest request) {
        return iotdbService.getAnomalyDetectionResult(id, request);
    }

    @GetMapping("/{id}/devices/overview")
    public List<IoTDBSeriesOverview> getDeviceOverview(
            @PathVariable int id,
            @RequestParam(value = "path", required = false) String path) {
        return iotdbService.getDeviceOverview(id, path);
    }

    @GetMapping("/{id}/databases/overview")
    public List<IoTDBSeriesOverview> getDatabaseOverview(
            @PathVariable int id,
            @RequestParam(value = "path", required = false) String path) {
        return iotdbService.getDatabaseOverview(id, path);
    }

    @GetMapping("/{id}/time-series/data")
    public TimeSeriesRecentDataDto getTimeSeriesData(
            @PathVariable int id,
            @RequestParam(required = false, defaultValue = "") String path,
            @RequestParam(required = false, defaultValue = "10") Long limit
    ) {
        return iotdbService.getTimeSeriesData(id, path, limit);
    }
}
