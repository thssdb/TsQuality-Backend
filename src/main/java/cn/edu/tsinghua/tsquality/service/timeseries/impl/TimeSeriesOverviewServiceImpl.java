package cn.edu.tsinghua.tsquality.service.timeseries.impl;

import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.repositories.impl.RepositoryImpl;
import cn.edu.tsinghua.tsquality.model.dto.timeseries.TimeSeriesOverviewDto;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesOverviewService;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.stereotype.Service;

@Service
public class TimeSeriesOverviewServiceImpl implements TimeSeriesOverviewService {
  private final SessionPool sessionPool;

  public TimeSeriesOverviewServiceImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  @Override
  public TimeSeriesOverviewDto getOverview(String path) {
    Repository repository = new RepositoryImpl(sessionPool, new Path(path, true));
    return TimeSeriesOverviewDto.builder()
        .count(repository.count(null))
        .minTimestamp(repository.selectMinTimestamp())
        .maxTimestamp(repository.selectMaxTimestamp())
        .build();
  }
}
