package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import cn.edu.tsinghua.tsquality.service.TimestampAnomalyService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TimestampAnomalyServiceImpl implements TimestampAnomalyService {
  @Override
  public List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path, Long startTimestamp, Long endTimestamp, Long standardInterval, String detectionMethod) {
    return null;
  }
}
