package cn.edu.tsinghua.tsquality.controller;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import cn.edu.tsinghua.tsquality.service.IoTDBConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/iotdb-config")
public class IoTDBConfigController {
    @Autowired private IoTDBConfigService ioTDBConfigService;

    @PostMapping()
    public int save(@RequestBody IoTDBConfig ioTDBConfig) {
        return ioTDBConfigService.save(ioTDBConfig);
    }

    @GetMapping("/all")
    public List<IoTDBConfig> getAll() {
        return ioTDBConfigService.getAll();
    }

    @GetMapping("/{id}")
    public IoTDBConfig getById(@PathVariable int id) {
        return ioTDBConfigService.getById(id);
    }

    @DeleteMapping("/{id}")
    public int deleteById(@PathVariable int id) {
        return ioTDBConfigService.deleteById(id);
    }
}
