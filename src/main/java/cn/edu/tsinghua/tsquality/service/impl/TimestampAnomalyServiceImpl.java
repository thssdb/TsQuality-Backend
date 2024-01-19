package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import cn.edu.tsinghua.tsquality.service.TimestampAnomalyService;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class TimestampAnomalyServiceImpl implements TimestampAnomalyService {
  @Override
  public List<TimestampAnomalyDto> anomalyDetectionAndRepair(
      String path,
      Long startTimestamp,
      Long endTimestamp,
      Long standardInterval,
      String detectionMethod) {
    return null;
  }
}
