package cn.edu.tsinghua.tsquality.model.dto.anomalies.timestamp;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.mappers.objects.Mapper;
import cn.edu.tsinghua.tsquality.mappers.objects.impl.TVListDataPointDtoListMapperImpl;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDataPointDto;
import java.util.List;
import lombok.Data;

@Data
public class TimestampAnomalyResultDto {
  private List<TimeSeriesDataPointDto> originalData;
  private List<TimeSeriesDataPointDto> repairedData;

  public TimestampAnomalyResultDto() {}

  public TimestampAnomalyResultDto(TVList originalData, TVList repairedData) {
    Mapper<TVList, List<TimeSeriesDataPointDto>> mapper = new TVListDataPointDtoListMapperImpl();
    this.originalData = mapper.mapTo(originalData);
    this.repairedData = mapper.mapTo(repairedData);
  }
}
