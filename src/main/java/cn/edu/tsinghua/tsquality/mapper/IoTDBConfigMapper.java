package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import java.util.List;
import org.apache.ibatis.annotations.*;
import org.springframework.web.bind.annotation.PathVariable;

@Mapper
public interface IoTDBConfigMapper {
  // create iotdb_config table if not exists
  void createIoTDBConfigTable();

  // get iotdb-config by id (without password)
  @Select("SELECT id, host, port, username FROM iotdb_configs WHERE id = #{id}")
  IoTDBConfig getById(@PathVariable("id") int id);

  // get iotdb-config by id (with password)
  @Select("SELECT * FROM iotdb_configs WHERE id = #{id}")
  IoTDBConfig getWithPasswordById(int id);

  // get all iotdb-configs
  @Select("SELECT id, host, port, username FROM iotdb_configs")
  List<IoTDBConfig> getAll();

  // create a new iotdb-config
  void create(@Param("config") IoTDBConfig config);

  // update an existing iotdb-config
  int update(IoTDBConfig config);

  // delete an iotdb-config
  @Delete("DELETE FROM iotdb_configs WHERE id = #{id}")
  int deleteById(@Param("id") int id);
}
