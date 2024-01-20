package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import cn.edu.tsinghua.tsquality.service.TimestampAnomalyService;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class TimestampAnomalyServiceImpl implements TimestampAnomalyService {

  @Override
  public List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path) {
    return new ArrayList<>();
  }

  @Override
  public List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path, Long standardInterval) {
    return null;
  }

  @Override
  public List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path, String detectionMethod) {
    return null;
  }

  private List<TimestampAnomalyDto> anomalyDetectionAndRepair(
      String path, Long standardInterval, String detectionMethod) {
    return new ArrayList<>();
  }
}
