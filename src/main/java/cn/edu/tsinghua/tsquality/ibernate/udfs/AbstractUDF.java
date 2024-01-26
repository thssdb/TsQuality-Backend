package cn.edu.tsinghua.tsquality.ibernate.udfs;

import java.util.Map;
import org.apache.iotdb.tsfile.read.common.Path;

public abstract class AbstractUDF {
  protected final String name;
  protected final Map<String, Object> params;

  protected AbstractUDF(String name, Map<String, Object> params) {
    this.name = name;
    this.params = params;
    if (!paramsValid()) {
      throw new IllegalArgumentException(params.toString());
    }
  }

  protected abstract boolean paramsValid();

  public String getSql(String path) {
    return getSql(new Path(path, true));
  }

  public String getSql(Path path) {
    String device = path.getDevice();
    String measurement = path.getMeasurement();
    String paramsString = paramsToString();
    return String.format("select %s(%s%s) from %s", name, measurement, paramsString, device);
  }

  private String paramsToString() {
    if (params == null || params.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Object> entry : params.entrySet()) {
      String keyValueString = String.format(",'%s'='%s'", entry.getKey(), entry.getValue());
      sb.append(keyValueString);
    }
    return sb.toString();
  }
}
