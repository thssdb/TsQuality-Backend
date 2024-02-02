package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TimeSeriesRecentDataDto {
  private String path;
  private List<TimeSeriesDataPointDto> points;

  public TimeSeriesRecentDataDto(String path, TVList tvList) {
    this.path = path;
    this.points = tvList.getPairs().stream().map(TimeSeriesDataPointDto::from).toList();
  }
}
