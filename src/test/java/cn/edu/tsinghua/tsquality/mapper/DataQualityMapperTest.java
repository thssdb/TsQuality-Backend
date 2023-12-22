package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.model.entity.SeriesStatWithTime;
import cn.edu.tsinghua.tsquality.model.enums.DQAggregationType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class DataQualityMapperTest {
    @Autowired PreAggregationConfig config;
    @Autowired DataQualityMapper mapper;

    @Test
    void selectSeriesStatByPath() {
        List<IoTDBSeriesStat> stats =
                mapper.selectSeriesStat(
                        config.tables.series, config.tables.fileSeriesStat, "root.sg2.d2.s1");
        System.out.println(stats);
    }

    @Test
    void selectDeviceStatByPath() {
        List<IoTDBSeriesStat> stats =
                mapper.selectDeviceStat(
                        config.tables.series, config.tables.fileSeriesStat, "root.sg2.d2");
        System.out.println(stats);
    }

    @Test
    void selectDatabaseStatByPath() {
        List<IoTDBSeriesStat> stats =
                mapper.selectDatabaseStat(
                        config.tables.series, config.tables.fileSeriesStat, "root.sg2.d2");
        System.out.println(stats);
    }

    @Test
    void testGetDataQualityFromFileStat() {
        List<SeriesStatWithTime> stats =
            mapper.getDataQualityAggregationDetailFromFileStats(
                "root.eastsidebuilding.device1.AB", DQAggregationType.MONTH.getType());
        System.out.println(stats);
    }
}
