package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionRequest;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesAnomalyDetectionResult;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesOverview;
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

    @GetMapping("/{id}/device/count")
    public long getNumsDevices(@PathVariable int id) {
        return iotdbService.getNumsDevices(id);
    }

    @GetMapping("/{id}/database/count")
    public long getNumsDatabases(@PathVariable int id) {
        return iotdbService.getNumsDatabases(id);
    }

    @GetMapping("/{id}/storage-group/count")
    public long getNumsStorageGroups(@PathVariable int id) {
        return iotdbService.getNumsStorageGroups(id);
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

    @GetMapping("/{id}/device/overview")
    public List<IoTDBSeriesOverview> getDeviceOverview(
            @PathVariable int id,
            @RequestParam(value = "path", required = false) String path) {
        return iotdbService.getDeviceOverview(id, path);
    }

    @GetMapping("/{id}/database/overview")
    public List<IoTDBSeriesOverview> getDatabaseOverview(
            @PathVariable int id,
            @RequestParam(value = "path", required = false) String path) {
        return iotdbService.getDatabaseOverview(id, path);
    }
}
