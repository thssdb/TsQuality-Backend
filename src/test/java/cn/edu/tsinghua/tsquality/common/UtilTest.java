package cn.edu.tsinghua.tsquality.common;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class UtilTest {

  @Test
  void splitSeriesPath() {
    System.out.println(Arrays.toString(IoTDBUtil.splitSeriesPath("root.sg0.d0.s0")));
  }

  @Test
  void formatTimestamp() {
    System.out.println(Util.formatTimestamp(1600000000000L));
  }
}
