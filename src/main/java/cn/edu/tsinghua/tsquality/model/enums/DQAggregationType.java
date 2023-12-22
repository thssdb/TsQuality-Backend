package cn.edu.tsinghua.tsquality.model.enums;

import lombok.Getter;

@Getter
public enum DQAggregationType {
    DAY("day"),
    MONTH("month"),

    YEAR("year");

    private final String type;

    DQAggregationType(String type) {
        this.type = type;
    }
}
