package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IoTDBMapper {
    @Autowired
    PreAggregationConfig config;

    @Autowired
    IoTDBSeriesMapper seriesMapper;
    @Autowired
    IoTDBFileMapper fileMapper;
    @Autowired
    IoTDBChunkMapper chunkMapper;
    @Autowired
    IoTDBPageMapper pageMapper;
    @Autowired
    IoTDBFileSeriesStatMapper fileSeriesStatMapper;
    @Autowired
    IoTDBChunkSeriesStatMapper chunkSeriesStatMapper;
    @Autowired
    IoTDBPageSeriesStatMapper pageSeriesStatMapper;

    public void createTablesIfNotExists() {
        PreAggregationConfig.TableNames tables = config.tables;
        seriesMapper.createSeriesTable(tables.series);
        fileMapper.createFileTable(tables.file);
        chunkMapper.createChunkTable(tables.chunk, tables.file);
        pageMapper.createPageTable(tables.page, tables.chunk);
        fileSeriesStatMapper.createFileSeriesStatTable(
                tables.file,
                tables.series,
                tables.fileSeriesStat
        );
        chunkSeriesStatMapper.createChunkSeriesStatTable(
                tables.chunk,
                tables.chunkSeriesStat
        );
        pageSeriesStatMapper.createPageSeriesStatTable(
                tables.page,
                tables.pageSeriesStat
        );
    }
}
