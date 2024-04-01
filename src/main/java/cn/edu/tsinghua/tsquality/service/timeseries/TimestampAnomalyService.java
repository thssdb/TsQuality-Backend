package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.request.TimestampAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.response.TimestampAnomalyResponseDto;
import org.apache.iotdb.tsfile.read.common.Path;

public interface TimestampAnomalyService {
  TimestampAnomalyResponseDto anomalyDetectionAndRepair(TimestampAnomalyRequestDto dto);

  TimestampAnomalyResponseDto anomalyDetectionAndRepair(String path, String timeFilter);

  TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      String path, Long standardInterval, String timeFilter);

  TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      String path, String detectionMethod, String timeFilter);

  default TimestampAnomalyResponseDto anomalyDetectionAndRepair(Path path, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), timeFilter);
  }

  default TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      Path path, Long standardInterval, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), standardInterval, timeFilter);
  }

  default TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      Path path, String detectionMethod, String timeFilter) {
    return anomalyDetectionAndRepair(path.getFullPath(), detectionMethod, timeFilter);
  }
}
