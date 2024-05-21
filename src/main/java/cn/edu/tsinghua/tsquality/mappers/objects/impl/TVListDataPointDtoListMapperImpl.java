package cn.edu.tsinghua.tsquality.mappers.objects.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.mappers.objects.Mapper;
import cn.edu.tsinghua.tsquality.model.dto.timeseries.TimeSeriesDataPointDto;
import java.util.ArrayList;
import java.util.List;

public class TVListDataPointDtoListMapperImpl
    implements Mapper<TVList, List<TimeSeriesDataPointDto>> {
  @Override
  public List<TimeSeriesDataPointDto> mapTo(TVList tvList) {
    int size = tvList.size();
    List<TimeSeriesDataPointDto> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add(new TimeSeriesDataPointDto(tvList.getTimestamp(i), tvList.getValue(i)));
    }
    return result;
  }

  @Override
  public TVList mapFrom(List<TimeSeriesDataPointDto> timeSeriesDataPointDtos) {
    throw new UnsupportedOperationException();
  }
}
