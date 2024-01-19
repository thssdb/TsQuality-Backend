package cn.edu.tsinghua.tsquality.generators;

import java.util.Arrays;

public class ValueGenerator {
  public static Double[] generateZeroDoubleValues(int size) {
    Double[] values = new Double[size];
    Arrays.fill(values, 0.0);
    return values;
  }
}
