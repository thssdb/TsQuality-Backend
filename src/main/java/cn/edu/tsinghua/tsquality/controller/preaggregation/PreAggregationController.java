package cn.edu.tsinghua.tsquality.controller.preaggregation;

import cn.edu.tsinghua.tsquality.model.dto.preaggregation.PreAggregationProgressDto;
import cn.edu.tsinghua.tsquality.service.preaggregation.PreAggregationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/pre-aggregation")
public class PreAggregationController {
  private final PreAggregationService service;

  public PreAggregationController(PreAggregationService service) {
    this.service = service;
  }

  @GetMapping("/progress")
  public PreAggregationProgressDto getProgress() {
    return service.getProgress();
  }
}
