package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.ibernate.udfs.ValueRepairUDF;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.value.ValueAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.timeseries.ValueAnomalyService;
import java.util.Map;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

@Service
public class ValueAnomalyServiceImpl implements ValueAnomalyService {
  private final SessionPool sessionPool;

  public ValueAnomalyServiceImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public ValueAnomalyResultDto anomalyDetectionAndRepair(String path, String timeFilter) {
    ValueRepairUDF udf = new ValueRepairUDF();
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  @Override
  public ValueAnomalyResultDto anomalyDetectionAndRepairWithScreen(
      String path, Double minSpeed, Double maxSpeed, String timeFilter) {
    ValueRepairUDF udf = new ValueRepairUDF(Map.of("minSpeed", minSpeed, "maxSpeed", maxSpeed));
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  @Override
  public ValueAnomalyResultDto anomalyDetectionAndRepairWithLsGreedy(
      String path, Double center, Double sigma, String timeFilter) {
    ValueRepairUDF udf = new ValueRepairUDF(Map.of("center", center, "sigma", sigma));
    return anomalyDetectionAndRepair(path, udf, timeFilter);
  }

  private ValueAnomalyResultDto anomalyDetectionAndRepair(
      String path, ValueRepairUDF udf, String timeFilter) {
    Repository repository = new RepositoryImpl(sessionPool, path);
    TVList original = repository.select(timeFilter, null);
    TVList repaired = repository.select(udf, timeFilter, null);
    return new ValueAnomalyResultDto(original, repaired);
  }
}
