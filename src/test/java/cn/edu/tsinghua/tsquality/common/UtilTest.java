package cn.edu.tsinghua.tsquality.common;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

class UtilTest {

    @Test
    void splitSeriesPath() {
        System.out.println(Arrays.toString(IoTDBUtil.splitSeriesPath("root.sg0.d0.s0")));
    }

    @Test
    void formatTimestamp() {
        System.out.println(Util.formatTimestamp(1600000000000L));
    }

    @Test
    void constructQuerySQL() {
        System.out.println(IoTDBUtil.constructQuerySQL("root.sg0.d0.s0", 1600000000000L,
                16900000000000L, "> 10.0"));
    }
}