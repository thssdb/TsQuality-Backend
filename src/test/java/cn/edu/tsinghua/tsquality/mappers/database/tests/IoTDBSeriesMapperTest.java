package cn.edu.tsinghua.tsquality.mappers.database.tests;

import cn.edu.tsinghua.tsquality.mappers.database.IoTDBSeriesMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@Transactional
class IoTDBSeriesMapperTest {
  @Autowired IoTDBSeriesMapper underTests;

  @Test
  void insertList() {
    List<IoTDBSeries> seriesList = new ArrayList<>();
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s0"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s1"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s2"));
    seriesList.add(new IoTDBSeries("root.test.g0.d0.s3"));
    underTests.insertList(seriesList);
  }

  @Test
  void selectIdByPath() {
    String path = "root.test.g0.d0.s2";
    System.out.println(underTests.selectIdByPath(path));
  }
}
