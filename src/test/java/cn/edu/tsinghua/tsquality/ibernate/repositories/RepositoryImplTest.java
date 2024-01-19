package cn.edu.tsinghua.tsquality.ibernate.repositories;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.ibernate.datacreators.IntTVListCreator;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.IntTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RepositoryImplTest {
  private static final String path = "root.tsquality.test.ts1";
  private static final String selectSql = "select ts1 from root.tsquality.test";

  private final RepositoryImpl underTests;

  private final Session session;

  RepositoryImplTest() throws IoTDBConnectionException {
    session = new Session.Builder().build();
    underTests = new RepositoryImpl(session, path, TSDataType.INT32);
  }

  @BeforeEach
  void createTimeSeries() {
    underTests.createTimeSeries();
  }

  @AfterEach
  void deleteTimeSeries() throws IoTDBConnectionException {
    underTests.deleteTimeSeries();
    session.close();
  }

  @Test
  void testRepositoryCanBeCreatedWithPath() throws Exception {
    Repository repository = givenRepositoryUsingPath();
    thenRepositoryIsNotNull(repository);
  }

  @Test
  void testRepositoryCanBeCreatedWithPathString() throws Exception {
    Repository repository = givenRepositoryUsingPathString();
    thenRepositoryIsNotNull(repository);
  }

  @Test
  void testCreateTimeSeriesShouldSucceed() throws Exception {
    underTests.createTimeSeries();
    assertThat(session.checkTimeseriesExists(path)).isTrue();
  }

  @Test
  void testDeleteTimeSeriesShouldSucceed() throws Exception {
    underTests.deleteTimeSeries();
    assertThat(session.checkTimeseriesExists(path)).isFalse();
  }

  @Test
  void testCreateAndDeleteTimeSeriesShouldSucceed() throws Exception {
    underTests.createTimeSeries();
    assertThat(session.checkTimeseriesExists(path)).isTrue();
    underTests.deleteTimeSeries();
    assertThat(session.checkTimeseriesExists(path)).isFalse();
  }

  @Test
  void testInsertShouldSucceed() throws Exception {
    IntTVList tvList = givenIntTVList();
    whenPerformInsert(tvList);
    thenInsertedDataShouldEqualToTVList(session, tvList);
  }

  @Test
  void testSelectAfterInsertShouldReturnCorrectData() {
    IntTVList tvList = givenIntTVList();
    TVList result = whenPerformSelectAfterInsert(tvList);
    thenSelectResultShouldBeEqualToTVList(result, tvList);
  }

  private Repository givenRepositoryUsingPath() throws Exception {
    return new RepositoryImpl(session, new Path(path), TSDataType.INT32);
  }

  private Repository givenRepositoryUsingPathString() throws Exception {
    return new RepositoryImpl(session, path, TSDataType.INT32);
  }

  private IntTVList givenIntTVList() {
    return IntTVListCreator.create(10);
  }

  private void whenPerformInsert(IntTVList tvList) {
    underTests.insert(tvList);
  }

  private TVList whenPerformSelectAfterInsert(TVList tvList) {
    underTests.createTimeSeries();
    underTests.insert(tvList);
    return underTests.select(null, null);
  }

  private void thenRepositoryIsNotNull(Repository repository) {
    assertThat(repository).isNotNull();
  }

  private void thenInsertedDataShouldEqualToTVList(Session session, IntTVList tvList)
      throws Exception {
    int index = 0;
    SessionDataSet.DataIterator iterator = session.executeQueryStatement(selectSql).iterator();
    while (iterator.next()) {
      long timestamp = iterator.getLong(1);
      int value = iterator.getInt(2);
      assertThat(timestamp).isEqualTo(tvList.getIntPair(index).getTimestamp());
      assertThat(value).isEqualTo(tvList.getIntPair(index).getInt());
      index++;
    }
  }

  private void thenSelectResultShouldBeEqualToTVList(TVList result, TVList tvList) {
    assertThat(result.size()).isEqualTo(tvList.size());
    for (int i = 0; i < result.size(); i++) {
      assertThat(result.getValue(i)).isEqualTo(tvList.getValue(i));
    }
  }
}
