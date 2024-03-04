package cn.edu.tsinghua.tsquality.ibernate.udfs;

import java.util.List;
import java.util.Map;

// UDF link: https://iotdb.apache.org/zh/UserGuide/latest/Reference/UDF-Libraries.html#valuerepair
public class ValueRepairUDF extends AbstractUDF {
  public static final String NAME = UDF.VALUE_REPAIR.getName();
  private static final String METHOD = "method";
  private static final List<String> ALLOWED_METHOD = List.of("Screen", "LsGreedy");
  private static final String MIN_SPEED = "minSpeed";
  private static final String MAX_SPEED = "maxSpeed";
  private static final String CENTER = "center";
  private static final String SIGMA = "sigma";

  public ValueRepairUDF() {
    super(NAME, null);
  }

  public ValueRepairUDF(Map<String, Object> params) {
    super(NAME, params);
  }

  @Override
  protected boolean paramsValid() {
    if (params == null) {
      return true;
    }
    if (!isValidMethod()) {
      return false;
    }
    return isScreenParamsValid() || isLsGreedyParamsValid();
  }

  private boolean isValidMethod() {
    Object value = params.get(METHOD);
    if (value == null) {
      return true;
    }
    if (!(value instanceof String method)) {
      return false;
    }
    return ALLOWED_METHOD.contains(method);
  }

  private boolean isScreenParamsValid() {
    Object method = params.get(METHOD);
    return (method == null || method.equals("Screen")) &&
        isValidDoubleParam(MIN_SPEED) && isValidMaxSpeed() &&
        !params.containsKey(CENTER) && !params.containsKey(SIGMA);
  }

  private boolean isValidMaxSpeed() {
    if (!isValidDoubleParam(MAX_SPEED)) {
      return false;
    }
    return params.get(MIN_SPEED) == null ||
        (Double) params.get(MAX_SPEED) >= (Double) params.get(MIN_SPEED);
  }

  private boolean isLsGreedyParamsValid() {
    Object method = params.get(METHOD);
    return method.equals("LsGreedy") &&
        isValidDoubleParam(CENTER) && isValidDoubleParam(SIGMA) &&
        !params.containsKey(MIN_SPEED) && !params.containsKey(MAX_SPEED);
  }

  private boolean isValidDoubleParam(String key) {
    Object value = params.get(key);
    if (value == null) {
      return true;
    }
    return value instanceof Double;
  }
}
