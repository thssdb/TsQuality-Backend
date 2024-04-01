package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.ibernate.udfs.TimestampRepairUDF;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.response.TimestampAnomalyResponseDto;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.request.TimestampAnomalyRequestDto;
import cn.edu.tsinghua.tsquality.service.timeseries.TimestampAnomalyService;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

import java.util.Map;

@Log4j2
@Service
public class TimestampAnomalyServiceImpl implements TimestampAnomalyService {
  private final SessionPool sessionPool;

  public TimestampAnomalyServiceImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      TimestampAnomalyRequestDto request) {
    if (request.getInterval() != null) {
      return anomalyDetectionAndRepair(request.getPath(), request.getInterval(), request.getTimeFilter());
    }
    if (request.getMethod() != null) {
      return anomalyDetectionAndRepair(request.getPath(), request.getMethod(), request.getTimeFilter());
    }
    return anomalyDetectionAndRepair(request.getPath(), request.getTimeFilter());
  }

  @Override
  public TimestampAnomalyResponseDto anomalyDetectionAndRepair(String path, String timeFilter) {
    TimestampRepairUDF udf = new TimestampRepairUDF();
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  @Override
  public TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      String path, Long interval, String timeFilter) {
    TimestampRepairUDF udf = new TimestampRepairUDF(Map.of("interval", interval));
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  @Override
  public TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      String path, String method, String timeFilter) {
    TimestampRepairUDF udf = new TimestampRepairUDF(Map.of("method", method));
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  private TimestampAnomalyResponseDto anomalyDetectionAndRepair(
      String path, TimestampRepairUDF udf, String timeFilter) {
    Repository repository = new RepositoryImpl(sessionPool, path);
    TVList originalData = repository.select(timeFilter, null);
    TVList repairedData = repository.select(udf, timeFilter, null);
    return new TimestampAnomalyResponseDto(originalData, repairedData);
  }
}
