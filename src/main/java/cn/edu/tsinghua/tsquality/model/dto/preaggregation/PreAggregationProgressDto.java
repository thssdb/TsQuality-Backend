package cn.edu.tsinghua.tsquality.model.dto.preaggregation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PreAggregationProgressDto {
  private long totalFileCount;
  private long processedFileCount;
}
