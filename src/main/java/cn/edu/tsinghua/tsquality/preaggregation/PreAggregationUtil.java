package cn.edu.tsinghua.tsquality.preaggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

@Log4j2
public class PreAggregationUtil {
    private static final String DATABASE_REGEX = ".*?/data/datanode/data/sequence/(.*?)/.*";

    public static List<TsFileInfo> getAllTsFiles(String dataDir) {
        Pattern dbPattern = Pattern.compile(DATABASE_REGEX);
        List<TsFileInfo> tsFiles = new ArrayList<>();
        File dir = new File(dataDir);
        File[] files = dir.listFiles();
        if (files == null) {
            return tsFiles;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                tsFiles.addAll(getAllTsFiles(file.getAbsolutePath()));
            } else if (file.getName().endsWith("tsfile")) {
                String filePath = file.getAbsolutePath();
                Matcher matcher = dbPattern.matcher(filePath);
                if (!matcher.matches()) {
                    log.warn("Cannot get database name from file path: " + filePath);
                    continue;
                }
                String database = matcher.group(1);
                long fileVersion = getFileVersion(filePath);
                tsFiles.add(
                        TsFileInfo.builder()
                                .filePath(filePath)
                                .fileVersion(fileVersion)
                                .database(database)
                                .build());
            }
        }
        return tsFiles;
    }

    public static long getFileVersion(String filePath) {
        TsFileResource tsFileResource = new TsFileResource(new File(filePath));
        return tsFileResource.getTsFileSize()
                + new File(tsFileResource.getModFile().getFilePath()).length();
    }

    public static Map<Long, IChunkReader> getChunkReaders(
            Path tsPath, TsFileSequenceReader reader, List<Modification> modifications)
            throws IOException {
        List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(tsPath, true);
        if (!modifications.isEmpty()) {
            ModificationUtils.modifyChunkMetaData(chunkMetadataList, modifications);
        }

        Map<Long, IChunkReader> chunkReaders = new HashMap<>();
        for (ChunkMetadata metadata : chunkMetadataList) {
            Chunk chunk = reader.readMemChunk(metadata);
            IChunkReader chunkReader = new ChunkReader(chunk, null);
            chunkReaders.put(metadata.getOffsetOfChunkHeader(), chunkReader);
        }
        return chunkReaders;
    }
}
