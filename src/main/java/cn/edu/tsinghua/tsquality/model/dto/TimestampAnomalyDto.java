package cn.edu.tsinghua.tsquality.model.dto;

import lombok.Data;

@Data
public class TimestampAnomalyDto {
  private long originalTimestamp;
  private long repairedTimestamp;
}
