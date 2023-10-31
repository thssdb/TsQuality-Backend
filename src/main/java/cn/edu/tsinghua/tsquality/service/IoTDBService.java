package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.mapper.IoTDBMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import cn.edu.tsinghua.tsquality.preaggregation.TsFileStat;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
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

    @Value("${pre-aggregation.data-dir}")
    public String dataDir = ".";

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
        Map<String, Long> tsFiles = UtilFunc.getAllTsFiles(dataDir);
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
                List<IChunkReader> chunkReaders = UtilFunc.getChunkReaders(path, reader, modifications);

                for (int i = 0; i < chunkReaders.size(); i++) {
                    tsFileStat.startNewChunk();
                    List<IPageReader> pageReaders = chunkReaders.get(i).loadPageReaderList();
                    try {
                        for (int j = 0; j < pageReaders.size(); j++) {
                            BatchData batchData = pageReaders.get(i).getAllSatisfiedPageData();
                            if (batchData.getDataType() == TSDataType.VECTOR) {
                                // ignore vector type
                                continue;
                            }
                            TsFileStat.SeriesStat stat = new TsFileStat.SeriesStat(batchData);
                            tsFileStat.addPageSeriesStat(stat);
                        }
                        tsFileStat.endChunk();
                        seriesStatMap.put(path, tsFileStat);
                    } catch (IOException | ArrayIndexOutOfBoundsException e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    static class UtilFunc {
        public static Map<String, Long>  getAllTsFiles(String dataDir) {
            Map<String, Long> tsFiles = new HashMap<>();
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            if (files == null) {
                return tsFiles;
            }
            for (File file: files) {
                if (file.isDirectory()) {
                    tsFiles.putAll(getAllTsFiles(file.getAbsolutePath()));
                } else if (file.getName().endsWith("tsfile")) {
                    String filePath = file.getAbsolutePath();
                    long fileVersion = getFileVersion(filePath);
                    tsFiles.put(filePath, fileVersion);
                }
            }
            return tsFiles;
        }

        public static long getFileVersion(String filePath) {
            TsFileResource tsFileResource = new TsFileResource(new File(filePath));
            return tsFileResource.getTsFileSize() + new File(tsFileResource.getModFile().getFilePath()).length();
        }

        public static List<IChunkReader> getChunkReaders(
                Path tsPath, TsFileSequenceReader reader, List<Modification> modifications)
                throws IOException {
            List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(tsPath, true);
            if (!modifications.isEmpty()) {
                ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
            }

            List<IChunkReader> chunkReaders = new LinkedList<>();
            for (ChunkMetadata metadata : chunkMetadataList) {
                Chunk chunk = reader.readMemChunk(metadata);
                IChunkReader chunkReader = new ChunkReader(chunk, null);
                chunkReaders.add(chunkReader);
            }
            return chunkReaders;
        }

    }
}
