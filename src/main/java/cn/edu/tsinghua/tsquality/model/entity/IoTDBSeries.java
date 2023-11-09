package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBSeries {
    private int sid;
    private String path;
    private String device = "";
    private String database = "";

    public IoTDBSeries(String path) {
        this.path = path;
    }

    public IoTDBSeries(String path, String device) {
        this.path = path;
        this.device = device;
    }
}
