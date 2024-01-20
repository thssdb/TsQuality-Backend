package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public interface TimestampAnomalyService {
  List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path);

  List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path, Long standardInterval);

  List<TimestampAnomalyDto> anomalyDetectionAndRepair(String path, String detectionMethod);

  default List<TimestampAnomalyDto> anomalyDetectionAndRepair(Path path) {
    return anomalyDetectionAndRepair(path.getFullPath());
  }

  default List<TimestampAnomalyDto> anomalyDetectionAndRepair(Path path, Long standardInterval) {
    return anomalyDetectionAndRepair(path.getFullPath(), standardInterval);
  }

  default List<TimestampAnomalyDto> anomalyDetectionAndRepair(Path path, String detectionMethod) {
    return anomalyDetectionAndRepair(path.getFullPath(), detectionMethod);
  }
}
