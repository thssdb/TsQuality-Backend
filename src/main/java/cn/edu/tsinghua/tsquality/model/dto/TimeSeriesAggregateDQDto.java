package cn.edu.tsinghua.tsquality.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@Builder
public class TimeSeriesAggregateDQDto {
  private List<Item> items;

  public TimeSeriesAggregateDQDto(Map<String, List<Double>> map) {
    items = new ArrayList<>();


  }

  @AllArgsConstructor
  @Builder
  public static class Item {
    public String time;
    public List<Double> dqMetrics;
  }
}
