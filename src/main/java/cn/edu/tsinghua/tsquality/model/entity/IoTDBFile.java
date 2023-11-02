package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBFile {
    int fid;
    String filePath;
    long fileVersion;

    public IoTDBFile(String filePath, long fileVersion) {
        this.filePath = filePath;
        this.fileVersion = fileVersion;
    }
}
