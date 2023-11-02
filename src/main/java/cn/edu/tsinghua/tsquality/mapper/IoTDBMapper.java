package cn.edu.tsinghua.tsquality.mapper;

import cn.edu.tsinghua.tsquality.config.PreAggregationConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBChunk;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBFile;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeries;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import cn.edu.tsinghua.tsquality.preaggregation.Util;
import org.apache.iotdb.tsfile.read.common.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

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

    public void saveTsFileStat(
            String filePath, List<Path> seriesPaths, Map<Path, TsFileStat> stats) {
        PreAggregationConfig.TableNames tables = config.tables;
        IoTDBFile file = new IoTDBFile(filePath, Util.getFileVersion(filePath));
        int fid;
        // insert one file into the file table
        int res = fileMapper.insert(tables.file, file);
        if (res == 1) {
            // insert succeeded
            fid = file.getFid();
        } else {
            // already inserted before
            fid = fileMapper.selectIdByFilePath(tables.file, file.getFilePath());
        }
        // insert multiple series into the series table
        seriesMapper.insertList(
                tables.series,
                seriesPaths.stream().map(s -> new IoTDBSeries(s.getFullPath())).toList()
        );
        for (Map.Entry<Path, TsFileStat> entry: stats.entrySet()) {
            Path seriesPath = entry.getKey();
            int sid = seriesMapper.selectIdByPath(tables.series, seriesPath.getFullPath());
            TsFileStat stat = entry.getValue();
            IoTDBSeriesStat fileStat = stat.getFileStat();
            Map<Long, IoTDBSeriesStat> chunkStats = stat.getChunkStats();
            // insert one file-series stat into the file-series-stat table
            fileSeriesStatMapper.insert(tables.fileSeriesStat, fid, sid, fileStat);
            for (Map.Entry<Long, IoTDBSeriesStat> chunkEntry: chunkStats.entrySet()) {
                IoTDBChunk chunk = new IoTDBChunk(fid, sid, chunkEntry.getKey());
                chunkMapper.insert(tables.chunk, chunk);  // insert one chunk into the chunk table
                int cid = chunk.getCid();
                // insert one chunk-series stat into the chunk-series-stat table
                chunkSeriesStatMapper.insert(tables.chunkSeriesStat, cid, chunkEntry.getValue());
            }
        }
    }
}
