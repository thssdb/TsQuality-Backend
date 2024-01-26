package cn.edu.tsinghua.tsquality.ibernate.udfs;

import java.util.List;
import java.util.Map;

public class TimestampRepairUDF extends AbstractUDF {
  public static final String NAME = UDF.TIMESTAMP_REPAIR.getName();

  private static final String INTERVAL = "interval";
  private static final String METHOD = "method";
  private static final List<String> ALLOWED_METHOD = List.of("median", "mode", "cluster");

  public TimestampRepairUDF() {
    super(NAME, null);
  }

  public TimestampRepairUDF(Map<String, Object> params) {
    super(NAME, params);
  }

  @Override
  protected boolean paramsValid() {
    if (params == null) {
      return true;
    }
    if (params.size() != 1) {
      return false;
    }
    return isValidInterval() || isValidMethod();
  }

  private boolean isValidInterval() {
    if (!params.containsKey(INTERVAL)) {
      return false;
    }
    return params.get(INTERVAL) instanceof Number;
  }

  private boolean isValidMethod() {
    if (!params.containsKey(METHOD)) {
      return false;
    }
    Object value = params.get(METHOD);
    if (!(value instanceof String method)) {
      return false;
    }
    return ALLOWED_METHOD.contains(method);
  }
}
