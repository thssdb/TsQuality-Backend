package cn.edu.tsinghua.tsquality.service.preaggregation;

import cn.edu.tsinghua.tsquality.model.dto.preaggregation.PreAggregationProgressDto;

public interface PreAggregationService {
  void preAggregate() throws InterruptedException;

  PreAggregationProgressDto getProgress();
}
