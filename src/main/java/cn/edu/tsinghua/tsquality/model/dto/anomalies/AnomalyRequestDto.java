package cn.edu.tsinghua.tsquality.model.dto.anomalies;

import lombok.Data;

@Data
public class AnomalyRequestDto {
  protected String path;
  protected Long startTimestamp;
  protected Long endTimestamp;

  public String getTimeFilter() {
    String left = startTimestamp == null || startTimestamp == 0 ? "" : "time >= " + startTimestamp;
    String right = endTimestamp == null || endTimestamp == 0 ? "" : "time <= " + endTimestamp;
    if (left.isEmpty() && right.isEmpty()) {
      return "";
    } else if (left.isEmpty()) {
      return right;
    } else if (right.isEmpty()) {
      return left;
    }
    return left + " and " + right;
  }
}
