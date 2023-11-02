package cn.edu.tsinghua.tsquality.preaggregation;

import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Util {
    public static Map<String, Long> getAllTsFiles(String dataDir) {
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
