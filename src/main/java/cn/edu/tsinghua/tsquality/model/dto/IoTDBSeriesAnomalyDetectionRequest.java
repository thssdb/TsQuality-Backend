package cn.edu.tsinghua.tsquality.model.dto;

import lombok.Data;

@Data
public class IoTDBSeriesAnomalyDetectionRequest {
    private String seriesPath;
    private TimeFilter timeFilter;
    private ValueFilter valueFilter;
    private boolean isCompletenessConsidered;
    private boolean isConsistencyConsidered;
    private boolean isTimelinessConsidered;
    private boolean isValidityConsidered;
    private Completeness completeness;
    private Consistency consistency;
    private Timeliness timeliness;
    private Validity validity;

    @Data
    public static class TimeFilter {
        private long minTimestamp;
        private long maxTimestamp;
    }

    @Data
    public static class ValueFilter {
        private String content;
    }

    @Data
    public static class Completeness {
        private String window;
        private boolean isDowntimeConsidered;
        private long shortestDowntime;
    }

    @Data
    public static class Consistency {
        private String window;
    }

    @Data
    public static class Timeliness {
        private String window;
    }

    @Data
    public static class Validity {
        private String window;
        private boolean isValueConsidered;
        private boolean isVariationConsidered;
        private boolean isSpeedConsidered;
        private boolean isAccelerationConsidered;
        private double minValue;
        private double maxValue;
        private double minVariation;
        private double maxVariation;
        private double minSpeed;
        private double maxSpeed;
        private double minAcceleration;
        private double maxAcceleration;
    }
}
