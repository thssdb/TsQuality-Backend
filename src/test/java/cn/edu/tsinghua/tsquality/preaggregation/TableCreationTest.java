package cn.edu.tsinghua.tsquality.preaggregation;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.mappers.database.TableMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class TableCreationTest {
  @Autowired private TableMapper mapper;

  @Test
  void testStatsTablesCreationShouldSucceed() {
    List<String> tables = mapper.selectAllTables();
    assertThat(tables).contains("series");
    assertThat(tables).contains("files");
    assertThat(tables).contains("chunks");
    assertThat(tables).contains("pages");
    assertThat(tables).contains("file_series_stats");
    assertThat(tables).contains("chunk_series_stats");
    assertThat(tables).contains("page_series_stats");
  }
}
