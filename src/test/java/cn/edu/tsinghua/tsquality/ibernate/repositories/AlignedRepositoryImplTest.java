package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.AlignedRepositoryImpl;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
public class AlignedRepositoryImplTest {
  private static final List<Path> paths = IoTDBDataGenerator.getPaths();
  private AlignedRepositoryImpl underTests;

  @Autowired private SessionPool sessionPool;

  @BeforeEach
  void setup() {
    underTests = new AlignedRepositoryImpl(sessionPool, paths);
  }

  @Test
  void testCreateAlignedTimeSeriesShouldSucceed() throws IoTDBConnectionException, StatementExecutionException {
    underTests.createAlignedTimeSeries(IoTDBDataGenerator.getDataTypes());
    for (Path path : paths) {
      assertThat(sessionPool.checkTimeseriesExists(path.getFullPath())).isTrue();
    }
  }
}
