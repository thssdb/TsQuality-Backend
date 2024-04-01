package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.ibernate.udfs.ValueRepairUDF;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.request.ValueAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.response.ValueAnomalyResponseDto;
import cn.edu.tsinghua.tsquality.service.timeseries.ValueAnomalyService;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ValueAnomalyServiceImpl implements ValueAnomalyService {
  private final SessionPool sessionPool;

  public ValueAnomalyServiceImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public ValueAnomalyResponseDto anomalyDetectionAndRepair(ValueAnomalyRequestDto request) {
    Map<String, Object> args = stringToMap(request.getArgs());
    return anomalyDetectionAndRepair(args, request.getPath(), request.getTimeFilter());
  }

  private Map<String, Object> stringToMap(String args) {
    if (args == null || args.isEmpty()) {
      return new HashMap<>();
    }
    Map<String, Object> result = new HashMap<>();
    String[] pairs = args.split(",\\s*");
    for (String pair : pairs) {
      String[] keyValue = pair.split("=");
      if (keyValue.length == 2) {
        result.put(keyValue[0].trim(), keyValue[1].trim());
      }
    }
    return result;
  }

  @Override
  public ValueAnomalyResponseDto anomalyDetectionAndRepair(Map<String, Object> params, String path, String timeFilter) {
    ValueRepairUDF udf = new ValueRepairUDF(params);
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  private ValueAnomalyResponseDto anomalyDetectionAndRepair(
      String path, ValueRepairUDF udf, String timeFilter) {
    Repository repository = new RepositoryImpl(sessionPool, path);
    TVList original = repository.select(timeFilter, null);
    TVList repaired = repository.select(udf, timeFilter, null);
    return new ValueAnomalyResponseDto(original, repaired);
  }
}
