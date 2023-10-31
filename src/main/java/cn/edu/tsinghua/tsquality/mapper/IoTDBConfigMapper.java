package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import org.apache.ibatis.annotations.*;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;

@Mapper
public interface IoTDBConfigMapper {
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
    @Insert("INSERT INTO iotdb_configs (host, port, username, password) " +
            "VALUES (#{host}, #{port}, #{username}, #{password})")
    int insert(IoTDBConfig ioTDBConfig);


    // update an existing iotdb-config
    int update(IoTDBConfig ioTDBConfig);

    // delete an iotdb-config
    @Delete("DELETE FROM iotdb_configs WHERE id = #{id}")
    int deleteById(@Param("id") int id);
}
