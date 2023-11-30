package cn.edu.tsinghua.tsquality.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IoTDBDataPointDto {
    private long timestamp;
    private double value;
}
