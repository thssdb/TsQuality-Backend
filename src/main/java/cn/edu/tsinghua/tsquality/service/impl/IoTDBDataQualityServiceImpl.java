package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.model.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.TimeSeriesDataQualityService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IoTDBDataQualityServiceImpl implements TimeSeriesDataQualityService {
    @Override
    public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
            String path, DQAggregationType aggregationType, List<TimeRange> timeRanges) {
        return new TimeSeriesDQAggregationDetailDto();
    }
}
