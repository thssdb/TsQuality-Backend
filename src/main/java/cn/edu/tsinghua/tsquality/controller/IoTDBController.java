package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.service.IoTDBService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/iotdb")
public class IoTDBController {
    @Autowired
    private IoTDBService ioTDBService;

    @GetMapping("/{id}/time-series/count")
    public long getNumsTimeSeries(@PathVariable int id) {
        return ioTDBService.getNumsTimeSeries(id);
    }

    @GetMapping("/{id}/device/count")
    public long getNumsDevices(@PathVariable int id) {
        return ioTDBService.getNumsDevices(id);
    }

    @GetMapping("/{id}/database/count")
    public long getNumsDatabases(@PathVariable int id) {
        return ioTDBService.getNumsDatabases(id);
    }

    @GetMapping("/{id}/storage-group/count")
    public long getNumsStorageGroups(@PathVariable int id) {
        return ioTDBService.getNumsStorageGroups(id);
    }
}
