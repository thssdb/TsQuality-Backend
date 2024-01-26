package cn.edu.tsinghua.tsquality.ibernate.udfs;

import org.apache.iotdb.tsfile.read.common.Path;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TimestampRepairUDFTest {
  private TimestampRepairUDF underTests;
  private static final Path PATH = new Path("root.ln.wf01.wt01.wa01", true);

  @Test
  void testSqlGenerationWithoutParams() {
    setupUnderTestsWithoutParams();
    String actual = underTests.getSql(PATH);
    String expected =
        String.format(
            "select timestamprepair(%s) from %s", PATH.getMeasurement(), PATH.getDevice());
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testSqlGenerationWithInterval() {
    setupUnderTestsWithInterval();
    String actual = underTests.getSql(PATH);
    String expected =
        String.format(
            "select timestamprepair(%s,'interval'='1000') from %s",
            PATH.getMeasurement(), PATH.getDevice());
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testSqlGenerationWithMethod() {
    setupUnderTestsWithMethod();
    String actual = underTests.getSql(PATH);
    String expected =
        String.format(
            "select timestamprepair(%s,'method'='mode') from %s",
            PATH.getMeasurement(), PATH.getDevice());
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void testSqlGenerationWithInvalidInterval() {
    assertThatThrownBy(this::setupUnderTestsWithInvalidInterval)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("{interval=1000}");
  }

  @Test
  void testSqlGenerationWithInvalidMethodType() {
    assertThatThrownBy(this::setupUnderTestsWithInvalidMethodType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("{method=1000}");
  }

  @Test
  void testSqlGenerationWithInvalidMethodValue() {
    assertThatThrownBy(this::setupUnderTestsWithInvalidMethodValue)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("{method=invalid}");
  }

  private void setupUnderTestsWithoutParams() {
    underTests = new TimestampRepairUDF();
  }

  private void setupUnderTestsWithInterval() {
    Map<String, Object> params = Map.of("interval", 1000);
    underTests = new TimestampRepairUDF(params);
  }

  private void setupUnderTestsWithMethod() {
    Map<String, Object> params = Map.of("method", "mode");
    underTests = new TimestampRepairUDF(params);
  }

  private void setupUnderTestsWithInvalidInterval() {
    Map<String, Object> params = Map.of("interval", "1000");
    underTests = new TimestampRepairUDF(params);
  }

  private void setupUnderTestsWithInvalidMethodType() {
    Map<String, Object> params = Map.of("method", 1000);
    underTests = new TimestampRepairUDF(params);
  }

  private void setupUnderTestsWithInvalidMethodValue() {
    Map<String, Object> params = Map.of("method", "invalid");
    underTests = new TimestampRepairUDF(params);
  }
}
