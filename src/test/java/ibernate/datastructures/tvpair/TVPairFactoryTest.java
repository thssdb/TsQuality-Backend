package ibernate.datastructures.tvpair;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.jupiter.api.Test;

public class TVPairFactoryTest {
  @Test
  void testFactoryCreateCorrectPairType() {
    assertThat(TVPairFactory.createTVPair(TSDataType.BOOLEAN) instanceof BooleanTVPair).isTrue();
    assertThat(TVPairFactory.createTVPair(TSDataType.INT32) instanceof IntTVPair).isTrue();
    assertThat(TVPairFactory.createTVPair(TSDataType.INT64) instanceof LongTVPair).isTrue();
    assertThat(TVPairFactory.createTVPair(TSDataType.FLOAT) instanceof FloatTVPair).isTrue();
    assertThat(TVPairFactory.createTVPair(TSDataType.DOUBLE) instanceof DoubleTVPair).isTrue();
    assertThat(TVPairFactory.createTVPair(TSDataType.TEXT) instanceof BinaryTVPair).isTrue();
  }
}
