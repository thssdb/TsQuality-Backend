package cn.edu.tsinghua.tsquality.preaggregation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

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
        if (!isTsFileClosed(filePath)) {
          continue;
        }
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

  private static boolean isTsFileClosed(String tsFilePath) {
    String tsFileResourcePath = tsFilePath + ".resource";
    File tsFileResource = new File(tsFileResourcePath);
    return tsFileResource.exists();
  }

  public static long getFileVersion(String filePath) {
    TsFileResource tsFileResource = new TsFileResource(new File(filePath));
    return tsFileResource.getTsFileSize()
        + new File(tsFileResource.getModFile().getFilePath()).length();
  }
}
