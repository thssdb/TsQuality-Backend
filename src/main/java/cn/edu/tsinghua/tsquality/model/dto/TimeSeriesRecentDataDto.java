package cn.edu.tsinghua.tsquality.model.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TimeSeriesRecentDataDto {
    private String path;
    private List<IoTDBDataPointDto> points;
}
