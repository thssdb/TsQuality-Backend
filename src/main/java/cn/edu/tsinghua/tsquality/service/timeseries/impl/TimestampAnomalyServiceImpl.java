package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.ibernate.udfs.TimestampRepairUDF;
import cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.TimestampAnomalyResultDto;
import cn.edu.tsinghua.tsquality.service.timeseries.TimestampAnomalyService;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class TimestampAnomalyServiceImpl implements TimestampAnomalyService {
  private final SessionPool sessionPool;

  public TimestampAnomalyServiceImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public TimestampAnomalyResultDto anomalyDetectionAndRepair(String path) {
    TimestampRepairUDF udf = new TimestampRepairUDF();
    return anomalyDetectionAndRepair(path, udf);
  }

  @Override
  public TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, Long interval) {
    TimestampRepairUDF udf = new TimestampRepairUDF(Map.of("interval", interval));
    return anomalyDetectionAndRepair(path, udf);
  }

  @Override
  public TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, String method) {
    TimestampRepairUDF udf = new TimestampRepairUDF(Map.of("method", method));
    return anomalyDetectionAndRepair(path, udf);
  }

  private TimestampAnomalyResultDto anomalyDetectionAndRepair(String path, TimestampRepairUDF udf) {
    Repository repository = new RepositoryImpl(sessionPool, path);
    TVList originalData = repository.select(null, null);
    TVList repairedData = repository.select(udf, null, null);
    return new TimestampAnomalyResultDto(originalData, repairedData);
  }
}
