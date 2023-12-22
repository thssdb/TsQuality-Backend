package cn.edu.tsinghua.tsquality.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@Builder
public class TimeSeriesDQAggregationDetailDto {
    private List<Item> items;

    public TimeSeriesDQAggregationDetailDto() {
        items = new ArrayList<>();
    }

    @AllArgsConstructor
    @Builder
    public static class Item {
        public String time;
        public Long dataSize;
        public Double completeness;
        public Double consistency;
        public Double timeliness;
        public Double validity;
    }

    public void merge(TimeSeriesDQAggregationDetailDto other) {}
}
