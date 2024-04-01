package cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp.response;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.mappers.objects.Mapper;
import cn.edu.tsinghua.tsquality.mappers.objects.impl.TVListDataPointDtoListMapperImpl;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDataPointDto;
import lombok.Data;

import java.util.List;

@Data
public class TimestampAnomalyResponseDto {
  private List<TimeSeriesDataPointDto> originalData;
  private List<TimeSeriesDataPointDto> repairedData;

  public TimestampAnomalyResponseDto() {}

  public TimestampAnomalyResponseDto(TVList originalData, TVList repairedData) {
    Mapper<TVList, List<TimeSeriesDataPointDto>> mapper = new TVListDataPointDtoListMapperImpl();
    this.originalData = mapper.mapTo(originalData);
    this.repairedData = mapper.mapTo(repairedData);
  }
}
