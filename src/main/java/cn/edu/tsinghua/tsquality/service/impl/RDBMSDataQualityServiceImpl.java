package cn.edu.tsinghua.tsquality.service.impl;

import cn.edu.tsinghua.tsquality.mapper.DataQualityMapper;
import cn.edu.tsinghua.tsquality.model.datastructures.TimeRange;
import cn.edu.tsinghua.tsquality.model.dto.TimeSeriesDQAggregationDetailDto;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import cn.edu.tsinghua.tsquality.service.TimeSeriesDataQualityService;
import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Getter
@Service
public class RDBMSDataQualityServiceImpl implements TimeSeriesDataQualityService {

    private List<TimeRange> remainingTimeRanges = new ArrayList<>();

    private final DataQualityMapper dataQualityMapper;

    public RDBMSDataQualityServiceImpl(DataQualityMapper dataQualityMapper) {
        this.dataQualityMapper = dataQualityMapper;
    }

    @Override
    public TimeSeriesDQAggregationDetailDto getTimeSeriesDQAggregationDetail(
            String path, DQAggregationType aggregationType, List<TimeRange> timeRanges) {
        return switch (aggregationType) {
            case DAY -> getDQAggregationDetailByDay(path, timeRanges);
            case MONTH -> getDQAggregationDetailByMonth(path, timeRanges);
            case YEAR -> getDQAggregationDetailByYear(path, timeRanges);
            default -> throw new IllegalArgumentException(
                    "Invalid aggregation type: " + aggregationType);
        };
    }

    private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByDay(String path, List<TimeRange> timeRanges) {
        return new TimeSeriesDQAggregationDetailDto();
    }

    private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByMonth(String path, List<TimeRange> timeRanges) {
        return new TimeSeriesDQAggregationDetailDto();
    }

    private TimeSeriesDQAggregationDetailDto getDQAggregationDetailByYear(String path, List<TimeRange> timeRanges) {
        return new TimeSeriesDQAggregationDetailDto();
    }
}
