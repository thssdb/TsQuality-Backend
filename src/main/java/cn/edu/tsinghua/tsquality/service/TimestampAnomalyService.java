package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.model.dto.TimestampAnomalyDto;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public interface TimestampAnomalyService {
  List<TimestampAnomalyDto> anomalyDetectionAndRepair(
      String path,
      Long startTimestamp,
      Long endTimestamp,
      Long standardInterval,
      String detectionMethod
  );

  default List<TimestampAnomalyDto> anomalyDetectionAndRepair(
      Path path,
      Long startTimestamp,
      Long endTimestamp,
      Long standardInterval,
      String detectionMethod
  ) {
    return anomalyDetectionAndRepair(
        path.getFullPath(),
        startTimestamp,
        endTimestamp,
        standardInterval,
        detectionMethod
    );
  }
}
