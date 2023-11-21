package cn.edu.tsinghua.tsquality.model.dto;

import cn.edu.tsinghua.tsquality.model.entity.IoTDBTimeValuePair;
import lombok.Data;

import java.util.List;

@Data
public class IoTDBSeriesAnomalyDetectionResult {
    private boolean isCompletenessConsidered;
    private boolean isConsistencyConsidered;
    private boolean isTimelinessConsidered;
    private boolean isValidityConsidered;
    private Completeness completeness;
    private Consistency consistency;
    private Timeliness timeliness;
    private Validity validity;

    public IoTDBSeriesAnomalyDetectionResult(IoTDBSeriesAnomalyDetectionRequest request) {
        this.isCompletenessConsidered = request.isCompletenessConsidered();
        this.isConsistencyConsidered = request.isConsistencyConsidered();
        this.isTimelinessConsidered = request.isTimelinessConsidered();
        this.isValidityConsidered = request.isValidityConsidered();
        if (isCompletenessConsidered) {
            completeness = new Completeness();
        }
        if (isConsistencyConsidered) {
            consistency = new Consistency();
        }
        if (isTimelinessConsidered) {
            timeliness = new Timeliness();
        }
        if (isValidityConsidered) {
            validity = new Validity();
        }
    }

    @Data
    public static class Completeness {
        private IoTDBTimeValuePair[] result;
    }

    @Data
    public static class Consistency {
        private IoTDBTimeValuePair[] result;
    }

    @Data
    public static class Timeliness {
        private IoTDBTimeValuePair[] result;
    }

    @Data
    public static class Validity {
        private boolean isValueConsidered;
        private boolean isVariationConsidered;
        private boolean isSpeedConsidered;
        private boolean isAccelerationConsidered;
        private IoTDBTimeValuePair[] valueResult;
        private IoTDBTimeValuePair[] variationResult;
        private IoTDBTimeValuePair[] speedResult;
        private IoTDBTimeValuePair[] accelerationResult;
    }

    public void anomalyDetect(List<IoTDBTimeValuePair> pairs) {

    }
}
