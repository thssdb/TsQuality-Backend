package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.mapper.IoTDBMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import cn.edu.tsinghua.tsquality.preaggregation.Util;
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
import org.springframework.beans.factory.annotation.Autowired;
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

    @Value("${pre-aggregation.data-dir:.}")
    public String dataDir;

    @Autowired
    IoTDBMapper iotdbMapper;

    @Autowired
    IoTDBConfigMapper ioTDBConfigMapper;

    private static Session buildSession(IoTDBConfig ioTDBConfig) {
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
        IoTDBConfig ioTDBConfig = ioTDBConfigMapper.getWithPasswordById(iotdbConfigID);
        try (Session session = buildSession(ioTDBConfig)) {
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

    @PostConstruct
    private void startPreAggregation() {
        iotdbMapper.createTablesIfNotExists();
        Map<String, Long> tsFiles = Util.getAllTsFiles(dataDir);
        if (tsFiles.isEmpty()) {
            return;
        }
        for (String filePath: tsFiles.keySet()) {
            preAggregateTsFile(filePath);
        }
    }

    @Async("preAggregationTaskExecutor")
    public void preAggregateTsFile(String filePath) {
        Collection<Modification> allModifications =
                new TsFileResource(new File(filePath)).getModFile().getModifications();
        try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
            List<Path> seriesPaths = reader.getAllPaths();
            Map<Path, TsFileStat> seriesStatMap = new HashMap<>();
            for (Path path: seriesPaths) {
                if (path.getMeasurement().isEmpty()) {
                    continue;
                }
                TsFileStat tsFileStat = new TsFileStat(path);
                List<Modification> modifications = new ArrayList<>();
                for (Modification modification: allModifications) {
                    if (modification.getPath().matchFullPath((PartialPath) path)) {
                        modifications.add(modification);
                    }
                }
                Map<Long, IChunkReader> chunkReaders = Util.getChunkReaders(path, reader, modifications);
                for (Map.Entry<Long, IChunkReader> entry: chunkReaders.entrySet()) {
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
            iotdbMapper.saveTsFileStat(filePath, seriesPaths, seriesStatMap);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
