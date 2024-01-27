package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.mapper.database.IoTDBSeriesMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class IoTDBSeriesMapperTest {
  @Autowired IoTDBSeriesMapper mapper;

  @Test
  void insertList() {
    List<IoTDBSeries> seriesList = new ArrayList<>();
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s0"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s1"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s2"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s3"));
    mapper.insertList(seriesList);
  }

  @Test
  void selectIdByPath() {
    String path = "root.test.g0.d0.s2";
    int id = mapper.selectIdByPath(path);
    System.out.println(id);
  }
}
