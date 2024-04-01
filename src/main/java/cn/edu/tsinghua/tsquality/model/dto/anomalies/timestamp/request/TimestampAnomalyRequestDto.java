package cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.request;

import lombok.Data;

@Data
public class TimestampAnomalyRequestDto {
  private String path;
  private Long startTimestamp;
  private Long endTimestamp;
  private String method;
  private Long interval;

  public String getTimeFilter() {
    String left = startTimestamp == null ? "" : "time >= " + startTimestamp;
    String right = endTimestamp == null ? "" : "time <= " + endTimestamp;
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
