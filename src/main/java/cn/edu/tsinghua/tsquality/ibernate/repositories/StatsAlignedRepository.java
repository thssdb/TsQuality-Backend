package cn.edu.tsinghua.tsquality.ibernate.repositories;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;

import java.util.List;

public interface StatsAlignedRepository extends AlignedRepository {
  IoTDBSeriesStat selectStats(List<TimeRange> timeRanges);

  List<TimeRange> selectTimeRanges(List<TimeRange> timeRanges);
}
