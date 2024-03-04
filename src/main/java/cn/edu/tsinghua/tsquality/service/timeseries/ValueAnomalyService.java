package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.ValueAnomalyResultDto;

public interface ValueAnomalyService {
  ValueAnomalyResultDto anomalyDetectionAndRepair(String path, String timeFilter);

  ValueAnomalyResultDto anomalyDetectionAndRepairWithScreen(
      String path, Double minSpeed, Double maxSpeed, String timeFilter);

  ValueAnomalyResultDto anomalyDetectionAndRepairWithLsGreedy(
      String path, Double center, Double sigma, String timeFilter);
}
