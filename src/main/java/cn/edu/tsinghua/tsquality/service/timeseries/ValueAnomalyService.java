package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.request.ValueAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.response.ValueAnomalyResponseDto;

import java.util.Map;

public interface ValueAnomalyService {
  ValueAnomalyResponseDto anomalyDetectionAndRepair(ValueAnomalyRequestDto dto);

  ValueAnomalyResponseDto anomalyDetectionAndRepair(Map<String, Object> params, String path, String timeFilter);

}
