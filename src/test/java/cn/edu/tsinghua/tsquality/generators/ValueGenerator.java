package cn.edu.tsinghua.tsquality.generators;

import java.util.Arrays;
import org.springframework.stereotype.Component;

@Component
public class ValueGenerator {
  public Double[] zeroDoubleValues(int size) {
    Double[] values = new Double[size];
    Arrays.fill(values, 0.0);
    return values;
  }

  public Double[] linearDoubleValues(int size) {
    Double[] values = new Double[size];
    for (int i = 0; i < size; i++) {
      values[i] = (double) i;
    }
    return values;
  }
}
