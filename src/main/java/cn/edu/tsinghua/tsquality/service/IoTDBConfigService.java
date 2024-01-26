package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.mapper.database.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class IoTDBConfigService {
  @Autowired private IoTDBConfigMapper ioTDBConfigMapper;

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
    if (ioTDBConfig.getId() <= 0) {
      return create(ioTDBConfig);
    }
    return update(ioTDBConfig);
  }

  private int create(IoTDBConfig config) {
    ioTDBConfigMapper.create(config);
    return config.getId();
  }

  private int update(IoTDBConfig ioTDBConfig) {
    ioTDBConfigMapper.update(ioTDBConfig);
    return ioTDBConfig.getId();
  }

  public int deleteById(int id) {
    return ioTDBConfigMapper.deleteById(id);
  }
}
