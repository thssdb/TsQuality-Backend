package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import org.apache.iotdb.tsfile.read.common.Path;

public interface TimestampAnomalyService {
  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, String timeFilter);

  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, Long standardInterval, String timeFilter);

  TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, String detectionMethod, String timeFilter);

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), timeFilter);
  }

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path, Long standardInterval, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), standardInterval, timeFilter);
  }

  default TimestampAnomalyResultDto anomalyDetectionAndRepair(Path path, String detectionMethod, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), detectionMethod, timeFilter);
  }
}
