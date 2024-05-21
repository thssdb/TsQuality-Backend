package cn.edu.tsinghua.tsquality.controller.timeseries;

import cn.edu.tsinghua.tsquality.model.dto.timeseries.TimeSeriesOverviewDto;
import cn.edu.tsinghua.tsquality.service.timeseries.TimeSeriesOverviewService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/time-series")
public class TimeSeriesController {
  private final TimeSeriesOverviewService timeSeriesOverviewService;

  public TimeSeriesController(TimeSeriesOverviewService timeSeriesOverviewService) {
    this.timeSeriesOverviewService = timeSeriesOverviewService;
  }

  @GetMapping("/overview")
  public TimeSeriesOverviewDto getTimeSeriesOverview(@RequestParam("path") String path) {
    return timeSeriesOverviewService.getOverview(path);
  }
}
