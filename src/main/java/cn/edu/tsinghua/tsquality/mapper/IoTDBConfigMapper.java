package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface IoTDBConfigMapper {
    // get all iotdb-configs
    @Select("SELECT * FROM iotdb_configs")
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
