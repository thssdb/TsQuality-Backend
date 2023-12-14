package cn.edu.tsinghua.tsquality.model.entity;

import lombok.Data;

@Data
public class IoTDBConfig {
    int id;
    int port;
    String host;
    String username;
    String password;
}
