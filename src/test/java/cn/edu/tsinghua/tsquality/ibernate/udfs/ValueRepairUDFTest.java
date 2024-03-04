package cn.edu.tsinghua.tsquality.ibernate.udfs;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.jupiter.api.Test;

public class ValueRepairUDFTest {
  private static final Path PATH = new Path("root.ln.wf01.wt01.wa01", true);

  private ValueRepairUDF underTests;

  @Test
  void testSqlGenerationWithoutParamsShouldBeCorrect() {
    underTests = new ValueRepairUDF();
    String actual = underTests.getSql(PATH);
    String expected =
        String.format("select valuerepair(%s) from %s", PATH.getMeasurement(), PATH.getDevice());
    assertThat(actual).isEqualTo(expected);
  }
}
