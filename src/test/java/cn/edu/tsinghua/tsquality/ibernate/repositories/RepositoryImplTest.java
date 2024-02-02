package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.generators.IoTDBDataGenerator;
import cn.edu.tsinghua.tsquality.ibernate.datacreators.IntTVListCreator;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.IntTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.ibernate.udfs.TimestampRepairUDF;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;

@SpringBootTest
public class RepositoryImplTest {
  private static final String path = "root.tsquality.test.ts1";
  private static final String selectSql = "select ts1 from root.tsquality.test";

  private final RepositoryImpl underTests;

  @Autowired private IoTDBDataGenerator dataGenerator;

  private final Session session;

  RepositoryImplTest() throws IoTDBConnectionException {
    session = new Session.Builder().build();
    underTests = new RepositoryImpl(session, path);
  }

  @BeforeEach
  void createTimeSeries() {
    underTests.createTimeSeries(TSDataType.INT32);
  }

  @AfterEach
  void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    dataGenerator.deleteDatabase();
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
    underTests.createTimeSeries(TSDataType.INT32);
    assertThat(session.checkTimeseriesExists(path)).isTrue();
  }

  @Test
  void testDeleteTimeSeriesShouldSucceed() throws Exception {
    underTests.deleteTimeSeries();
    assertThat(session.checkTimeseriesExists(path)).isFalse();
  }

  @Test
  void testCreateAndDeleteTimeSeriesShouldSucceed() throws Exception {
    underTests.createTimeSeries(TSDataType.INT32);
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
  void testSelectShouldReturnCorrectData() {
    IntTVList tvList = givenIntTVList();
    TVList result = whenPerformSelectAfterInsert(tvList);
    thenSelectResultShouldBeEqualToTVList(result, tvList);
  }

  @Test
  void testTimestampRepairWithoutParamsShouldSucceed() {
    IntTVList tvList = givenIntTVList();
    TimestampRepairUDF udf = givenTimestampRepairUDFWithoutParams();
    TVList result = whenPerformTimestampRepairAfterInsert(tvList, udf);
    thenSelectResultShouldBeEqualToTVList(result, tvList);
  }

  @Test
  void testTimestampRepairWithIntervalShouldSucceed() {
    IntTVList tvList = givenIntTVList();
    TimestampRepairUDF udf = givenTimestampRepairUDFWithInterval();
    TVList result = whenPerformTimestampRepairAfterInsert(tvList, udf);
    thenSelectResultShouldBeOfSize(result, 2);
  }

  @Test
  void testTimestampRepairWithMethodShouldSucceed() {
    IntTVList tvList = givenIntTVList();
    TimestampRepairUDF udf = givenTimestampRepairUDFWithMethod();
    TVList result = whenPerformTimestampRepairAfterInsert(tvList, udf);
    thenSelectResultShouldBeEqualToTVList(result, tvList);
  }

  @Test
  void testFlushShouldSucceed() {
    IntTVList tvList = givenIntTVList();
    whenPerformInsert(tvList);
    thenFlushShouldSucceed();
  }

  private Repository givenRepositoryUsingPath() throws IoTDBConnectionException {
    return new RepositoryImpl(session, new Path(path));
  }

  private Repository givenRepositoryUsingPathString() throws Exception {
    return new RepositoryImpl(session, path);
  }

  private IntTVList givenIntTVList() {
    return IntTVListCreator.create(10);
  }

  private TimestampRepairUDF givenTimestampRepairUDFWithoutParams() {
    return new TimestampRepairUDF();
  }

  private TimestampRepairUDF givenTimestampRepairUDFWithInterval() {
    return new TimestampRepairUDF(Map.of("interval", 1000));
  }

  private TimestampRepairUDF givenTimestampRepairUDFWithMethod() {
    return new TimestampRepairUDF(Map.of("method", "mode"));
  }

  private void whenPerformInsert(IntTVList tvList) {
    underTests.insert(tvList);
  }

  private TVList whenPerformSelectAfterInsert(TVList tvList) {
    underTests.insert(tvList);
    return underTests.select(null, null);
  }

  private TVList whenPerformTimestampRepairAfterInsert(TVList tvList, TimestampRepairUDF udf) {
    underTests.insert(tvList);
    return underTests.select(udf, null, null);
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

  private void thenSelectResultShouldBeOfSize(TVList result, int size) {
    assertThat(result.size()).isEqualTo(size);
  }

  private void thenFlushShouldSucceed() {
    assertDoesNotThrow(underTests::flush);
  }
}
