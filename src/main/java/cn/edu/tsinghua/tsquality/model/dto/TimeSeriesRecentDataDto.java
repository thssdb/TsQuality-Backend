package cn.edu.tsinghua.tsquality.model.dto;

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
    private List<IoTDBDataPointDto> points;
}
