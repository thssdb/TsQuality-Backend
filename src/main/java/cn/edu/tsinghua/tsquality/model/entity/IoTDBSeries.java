package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBSeries {
    public int sid;
    public String path;

    public IoTDBSeries(String path) {
        this.path = path;
    }
}
