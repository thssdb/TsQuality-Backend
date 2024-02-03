package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public class AlignedRepositoryImpl implements AlignedRepository {
  private String device;
  private List<String> measurements;
  private final List<Path> paths;
  private final SessionPool sessionPool;

  public AlignedRepositoryImpl(SessionPool sessionPool, @Nonnull List<Path> paths) {
    if (!checkPathsNotEmptyAndHaveSameDevice(paths)) {
      throw new IllegalArgumentException("Paths must have the same device");
    }
    this.sessionPool = sessionPool;
    this.paths = paths;
    setDeviceAndMeasurements();
  }

  private boolean checkPathsNotEmptyAndHaveSameDevice(List<Path> paths) {
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("Paths cannot be empty");
    }
    String device = paths.getFirst().getDevice();
    for (Path path : paths) {
      if (!path.getDevice().equals(device)) {
        return false;
      }
    }
    return true;
  }

  private void setDeviceAndMeasurements() {
    device = paths.getFirst().getDevice();
    measurements = paths.stream().map(Path::getMeasurement).toList();
  }

  @Override
  public void createAlignedTimeSeries(List<TSDataType> dataTypes) throws IoTDBConnectionException, StatementExecutionException {
    List<TSEncoding> encodings = Collections.nCopies(paths.size(), TSEncoding.PLAIN);
    List<CompressionType> compressionTypes = Collections.nCopies(paths.size(), CompressionType.UNCOMPRESSED);
    sessionPool.createAlignedTimeseries(device, measurements, dataTypes, encodings, compressionTypes, null);
  }
}
