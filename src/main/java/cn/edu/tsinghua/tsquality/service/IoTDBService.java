package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.common.IoTDBUtil;
import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.mapper.IoTDBMapper;
import cn.edu.tsinghua.tsquality.model.dto.*;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBTimeValuePair;
import cn.edu.tsinghua.tsquality.preaggregation.PreAggregationUtil;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileInfo;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.*;


@Service
public class IoTDBService {
    public static final Logger LOGGER = LoggerFactory.getLogger(IoTDBService.class);
    private static final String SQL_QUERY_NUMS_TIME_SERIES = "COUNT TIMESERIES";
    private static final String SQL_QUERY_NUMS_DEVICES = "COUNT DEVICES";
    private static final String SQL_QUERY_NUMS_DATABASES = "COUNT DATABASES";
    private static final String SQL_QUERY_SHOW_TIME_SERIES = "SHOW TIMESERIES";

    @Value("${pre-aggregation.data-dir:.}")
    public String dataDir;

    final IoTDBMapper iotdbMapper;

    final IoTDBConfigMapper ioTDBConfigMapper;

    public IoTDBService(IoTDBMapper iotdbMapper, IoTDBConfigMapper ioTDBConfigMapper) {
        this.iotdbMapper = iotdbMapper;
        this.ioTDBConfigMapper = ioTDBConfigMapper;
    }

    public static Session buildSession(IoTDBConfig ioTDBConfig) {
        Session session;
        try {
            session = new Session.Builder()
                    .host(ioTDBConfig.getHost())
                    .port(ioTDBConfig.getPort())
                    .username(ioTDBConfig.getUsername())
                    .password(ioTDBConfig.getPassword())
                    .build();
        } catch (IllegalArgumentException e) {
            return null;
        }
        return session;
    }

    public long getCountResult(int iotdbConfigID, String sql) {
        IoTDBConfig iotdbConfig = ioTDBConfigMapper.getWithPasswordById(iotdbConfigID);
        if (iotdbConfig == null) {
            return 0;
        }
        try (Session session = buildSession(iotdbConfig)) {
            if (session == null) {
                return 0;
            }
            session.open();
            SessionDataSet dataSet = session.executeQueryStatement(sql);
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            if (iterator.next()) {
                return iterator.getLong(1);
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println(e.getMessage());
            return 0;
        }
        return 0;
    }

    public long getNumsTimeSeries(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_TIME_SERIES);
    }

    public long getNumsDevices(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DEVICES);
    }

    public long getNumsDatabases(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DATABASES);
    }

    public long getNumsStorageGroups(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DATABASES);
    }

    public List<IoTDBSeriesOverview> getTimeSeriesOverview(int id) {
        return iotdbMapper.selectSeriesStat().stream().map(IoTDBSeriesOverview::new).toList();
    }

    public List<IoTDBSeriesOverview> getDeviceOverview(int id, String path) {
        return iotdbMapper.selectDeviceStat(path).stream().map(IoTDBSeriesOverview::new).toList();
    }

    public List<IoTDBSeriesOverview> getDatabaseOverview(int id, String path) {
        return iotdbMapper.selectDatabaseStat(path).stream().map(IoTDBSeriesOverview::new).toList();
    }

    @PostConstruct
    private void startPreAggregation() {
        iotdbMapper.createTablesIfNotExists();
        List<TsFileInfo> tsFiles = PreAggregationUtil.getAllTsFiles(dataDir);
        if (tsFiles.isEmpty()) {
            return;
        }
        for (TsFileInfo tsfile : tsFiles) {
            preAggregateTsFile(tsfile);
        }
    }

    @Async("preAggregationTaskExecutor")
    public void preAggregateTsFile(TsFileInfo tsfile) {
        String filePath = tsfile.getFilePath();
        Collection<Modification> allModifications =
                new TsFileResource(new File(filePath)).getModFile().getModifications();
        try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
            List<Path> seriesPaths = reader.getAllPaths();
            Map<Path, TsFileStat> seriesStatMap = new HashMap<>();
            for (Path path : seriesPaths) {
                if (path.getMeasurement().isEmpty()) {
                    continue;
                }
                TsFileStat tsFileStat = new TsFileStat(path);
                List<Modification> modifications = new ArrayList<>();
                for (Modification modification : allModifications) {
                    if (modification.getPath().matchFullPath((PartialPath) path)) {
                        modifications.add(modification);
                    }
                }
                Map<Long, IChunkReader> chunkReaders = PreAggregationUtil.getChunkReaders(path, reader, modifications);
                for (Map.Entry<Long, IChunkReader> entry : chunkReaders.entrySet()) {
                    tsFileStat.startNewChunk(entry.getKey());
                    IChunkReader chunkReader = entry.getValue();
                    while (chunkReader.hasNextSatisfiedPage()) {
                        BatchData batchData = chunkReader.nextPageData();
                        if (batchData.getDataType() == TSDataType.VECTOR) {
                            // ignore vector type
                            continue;
                        }
                        IoTDBSeriesStat stat = new IoTDBSeriesStat(batchData);
                        tsFileStat.addPageSeriesStat(entry.getKey(), stat);
                    }
                    tsFileStat.endChunk(entry.getKey());
                }
                seriesStatMap.put(path, tsFileStat);
            }
            iotdbMapper.saveTsFileStat(tsfile, seriesPaths, seriesStatMap);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public IoTDBSeriesAnomalyDetectionResult getAnomalyDetectionResult(
            int id, IoTDBSeriesAnomalyDetectionRequest request
    ) {
        IoTDBSeriesAnomalyDetectionResult result = new IoTDBSeriesAnomalyDetectionResult(request);
        IoTDBConfig iotdbConfig = ioTDBConfigMapper.getWithPasswordById(id);
        if (iotdbConfig == null) {
            return result;
        }
        try (Session session = buildSession(iotdbConfig)) {
            if (session == null) {
                return result;
            }
            session.open();
            String sql = IoTDBUtil.constructQuerySQL(request.getSeriesPath(), request);
            SessionDataSet dataset = session.executeQueryStatement(sql);
            if (dataset.getColumnNames().size() != 2) {
                return result;
            }
            SessionDataSet.DataIterator iterator = dataset.iterator();
            List<IoTDBTimeValuePair> timeValuePairs = IoTDBTimeValuePair.buildFromDatasetIterator(iterator);
            result.anomalyDetect(timeValuePairs);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            return result;
        }
    }

    private void completenessAnomalyDetection(
            IoTDBSeriesAnomalyDetectionRequest request, IoTDBSeriesAnomalyDetectionResult result
    ) {

    }

    private void consistencyAnomalyDetection(
            IoTDBSeriesAnomalyDetectionRequest request, IoTDBSeriesAnomalyDetectionResult result
    ) {

    }

    private void timelinessAnomalyDetection(
            IoTDBSeriesAnomalyDetectionRequest request, IoTDBSeriesAnomalyDetectionResult result
    ) {

    }

    private void validityAnomalyDetection(
            IoTDBSeriesAnomalyDetectionRequest request, IoTDBSeriesAnomalyDetectionResult result
    ) {

    }

    public IoTDBAggregationInfoDto getAggregationInfo(int id) {
        IoTDBSeriesStat stat = iotdbMapper.selectAllStat();
        double completeness = DataQualityCalculationUtil.calculateCompleteness(stat);
        double consistency = DataQualityCalculationUtil.calculateConsistency(stat);
        double timeliness = DataQualityCalculationUtil.calculateTimeliness(stat);
        double validity = DataQualityCalculationUtil.calculateValidity(stat);
        return IoTDBAggregationInfoDto.builder()
                .numDataPoints(stat.getCnt())
                .numTimeSeries(getNumsTimeSeries(id))
                .numDevices(getNumsDevices(id))
                .numDatabases(getNumsDatabases(id))
                .completeness(completeness)
                .consistency(consistency)
                .timeliness(timeliness)
                .validity(validity)
                .build();
    }

    // get the full path of the latest time series with type == DOUBLE | FLOAT | INT32 | INT64
    public String getLatestNumericTimeSeriesPath(Session session)
            throws IoTDBConnectionException, StatementExecutionException {
        session.open();
        SessionDataSet.DataIterator iterator = session.executeQueryStatement(SQL_QUERY_SHOW_TIME_SERIES).iterator();
        while (iterator.next()) {
            if(IoTDBUtil.isNumericDataType(iterator.getString("DataType"))) {
                return iterator.getString("Timeseries");
            }
        }
        return "";
    }

    public TimeSeriesRecentDataDto getTimeSeriesData(int id, String path) {
        IoTDBConfig config = ioTDBConfigMapper.getWithPasswordById(id);
        if (config == null) {
            return new TimeSeriesRecentDataDto();
        }
        try (Session session = buildSession(config)) {
            if (session == null) {
                return new TimeSeriesRecentDataDto();
            }
            session.open();
            if (path == null || path.isEmpty()) {
                path = getLatestNumericTimeSeriesPath(session);
            }
            if (path.isEmpty()) {
                return new TimeSeriesRecentDataDto();
            }
            return IoTDBUtil.query(session, path);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            return new TimeSeriesRecentDataDto();
        }
    }

    public List<String> getLatestTimeSeriesPath(int id, String path, int limit) {
        IoTDBConfig config = ioTDBConfigMapper.getWithPasswordById(id);
        if (config == null) {
            return new ArrayList<>();
        }
        try (Session session = buildSession(config)) {
            if (session == null) {
                return new ArrayList<>();
            }
            session.open();
            return IoTDBUtil.showLatestTimeSeries(session, path, limit);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            return new ArrayList<>();
        }
    }
}
