package cn.edu.tsinghua.tsquality.mappers.objects.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPair;
import cn.edu.tsinghua.tsquality.mappers.objects.Mapper;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDataPointDto;
import org.springframework.stereotype.Component;

@Component
public class TVPairDataPointDtoMapperImpl implements Mapper<TVPair, TimeSeriesDataPointDto> {
  @Override
  public TimeSeriesDataPointDto mapTo(TVPair tvPair) {
    return new TimeSeriesDataPointDto(tvPair.getTimestamp(), tvPair.getValue());
  }

  @Override
  public TVPair mapFrom(TimeSeriesDataPointDto timeSeriesDataPointDto) {
    throw new UnsupportedOperationException();
  }
}
