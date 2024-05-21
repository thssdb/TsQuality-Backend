package cn.edu.tsinghua.tsquality.service.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.timeseries.TimeSeriesOverviewDto;

public interface TimeSeriesOverviewService {
  TimeSeriesOverviewDto getOverview(String path);
}
