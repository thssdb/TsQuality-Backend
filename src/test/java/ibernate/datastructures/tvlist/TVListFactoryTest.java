package ibernate.datastructures.tvlist;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.jupiter.api.Test;

public class TVListFactoryTest {
  @Test
  void testFactoryCreateCorrectListType() {
    assertThat(TVListFactory.createTVList(TSDataType.BOOLEAN) instanceof BooleanTVList).isTrue();
    assertThat(TVListFactory.createTVList(TSDataType.INT32) instanceof IntTVList).isTrue();
    assertThat(TVListFactory.createTVList(TSDataType.INT64) instanceof LongTVList).isTrue();
    assertThat(TVListFactory.createTVList(TSDataType.FLOAT) instanceof FloatTVList).isTrue();
    assertThat(TVListFactory.createTVList(TSDataType.DOUBLE) instanceof DoubleTVList).isTrue();
    assertThat(TVListFactory.createTVList(TSDataType.TEXT) instanceof BinaryTVList).isTrue();
  }
}
