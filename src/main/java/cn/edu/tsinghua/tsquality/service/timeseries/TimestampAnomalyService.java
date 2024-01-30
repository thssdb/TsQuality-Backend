package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import org.apache.iotdb.tsfile.read.common.Path;

public interface TimestampAnomalyService {
  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path);

  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, Long standardInterval);

  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, String detectionMethod);

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path) {
    return anomalyDetectionAndRepair(path.getFullPath());
  }

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path, Long standardInterval) {
    return anomalyDetectionAndRepair(path.getFullPath(), standardInterval);
  }

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path, String detectionMethod) {
    return anomalyDetectionAndRepair(path.getFullPath(), detectionMethod);
  }
}
