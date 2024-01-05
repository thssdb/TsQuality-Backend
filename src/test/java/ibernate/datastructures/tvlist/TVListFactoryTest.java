package ibernate.datastructures.tvlist;

import static org.assertj.core.api.Assertions.assertThat;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.jupiter.api.Test;

public class TVListFactoryTest {
  @Test
  void testFactoryCreateCorrectListType() {
    BooleanTVList boolList = (BooleanTVList) TVListFactory.createTVList(TSDataType.BOOLEAN);
    IntTVList intList = (IntTVList) TVListFactory.createTVList(TSDataType.INT32);
    LongTVList longList = (LongTVList) TVListFactory.createTVList(TSDataType.INT64);
    FloatTVList floatList = (FloatTVList) TVListFactory.createTVList(TSDataType.FLOAT);
    DoubleTVList doubleList = (DoubleTVList) TVListFactory.createTVList(TSDataType.DOUBLE);
    TextTVList binaryList = (TextTVList) TVListFactory.createTVList(TSDataType.TEXT);

    assertThat(boolList).isNotNull();
    assertThat(intList).isNotNull();
    assertThat(longList).isNotNull();
    assertThat(floatList).isNotNull();
    assertThat(doubleList).isNotNull();
  }
}
