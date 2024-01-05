package cn.edu.tsinghua.tsquality.mapper;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class IoTDBSeriesMapperTest {
  @Autowired IoTDBSeriesMapper mapper;

  @Autowired PreAggregationConfig config;

  @Test
  void insertList() {
    List<IoTDBSeries> seriesList = new ArrayList<>();
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s0"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s1"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s2"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s3"));
    mapper.insertList(config.tables.series, seriesList);
  }

  @Test
  void selectIdByPath() {
    String path = "root.test.g0.d0.s2";
    int id = mapper.selectIdByPath(config.tables.series, path);
    System.out.println(id);
  }
}
