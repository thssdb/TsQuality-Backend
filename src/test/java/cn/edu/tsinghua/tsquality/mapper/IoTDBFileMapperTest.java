package cn.edu.tsinghua.tsquality.mapper;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBFileMapperTest {
    @Autowired private IoTDBFileMapper ioTDBFileMapper;

    @Autowired private PreAggregationConfig config;

    @Test
    void insert() {
        IoTDBFile file = new IoTDBFile("test3.tsfile", 1);
        ioTDBFileMapper.insert(config.tables.file, file);
        System.out.println(file.getFid());
    }

    @Test
    void select() {
        Map<String, Object> res = ioTDBFileMapper.select(config.tables.file);
        System.out.println(res);
    }
}
