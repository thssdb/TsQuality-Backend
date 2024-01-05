package ibernate.datastructures.tvpair;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

public class TVPairFactoryTest {
  @Test
  void testFactoryCreateCorrectPairType() {
    BooleanTVPair boolPair = (BooleanTVPair) TVPairFactory.createTVPair(TSDataType.BOOLEAN);
    IntTVPair intPair = (IntTVPair) TVPairFactory.createTVPair(TSDataType.INT32);
    LongTVPair longPair = (LongTVPair) TVPairFactory.createTVPair(TSDataType.INT64);
    FloatTVPair floatPair = (FloatTVPair) TVPairFactory.createTVPair(TSDataType.FLOAT);
    DoubleTVPair doublePair = (DoubleTVPair) TVPairFactory.createTVPair(TSDataType.DOUBLE);
    TextTVPair binaryPair = (TextTVPair) TVPairFactory.createTVPair(TSDataType.TEXT);

    assertThat(boolPair).isNotNull();
    assertThat(intPair).isNotNull();
    assertThat(longPair).isNotNull();
    assertThat(floatPair).isNotNull();
    assertThat(doublePair).isNotNull();
    assertThat(binaryPair).isNotNull();
  }
}
