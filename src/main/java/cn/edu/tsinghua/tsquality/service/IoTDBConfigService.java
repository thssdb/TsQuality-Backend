package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IoTDBConfigService {
    @Autowired
    private IoTDBConfigMapper ioTDBConfigMapper;
    
    public List<IoTDBConfig> getAll() {
        return ioTDBConfigMapper.getAll();
    }

    public IoTDBConfig getById(int id) {
        return ioTDBConfigMapper.getById(id);
    }

    public IoTDBConfig getWithPasswordById(int id) {
        return ioTDBConfigMapper.getWithPasswordById(id);
    }

    public int save(IoTDBConfig ioTDBConfig) {
        if (ioTDBConfig.getId() == 0) {
            return create(ioTDBConfig);
        }
        return update(ioTDBConfig);
    }

    private int create(IoTDBConfig ioTDBConfig) {
        return ioTDBConfigMapper.insert(ioTDBConfig);
    }

    private int update(IoTDBConfig ioTDBConfig)  {
        return ioTDBConfigMapper.update(ioTDBConfig);
    }

    public int deleteById(int id) {
        return ioTDBConfigMapper.deleteById(id);
    }
}
