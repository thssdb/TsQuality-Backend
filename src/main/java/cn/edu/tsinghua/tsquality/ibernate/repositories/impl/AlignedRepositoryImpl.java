package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AlignedRepositoryImpl extends BaseRepository implements AlignedRepository {
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
  public void createAlignedTimeSeries(List<TSDataType> dataTypes)
      throws IoTDBConnectionException, StatementExecutionException {
    if (!dataTypesValid(dataTypes)) {
      throw new IllegalArgumentException("Data types must have the same size as measurements");
    }
    List<TSEncoding> encodings = Collections.nCopies(paths.size(), TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Collections.nCopies(paths.size(), CompressionType.UNCOMPRESSED);
    sessionPool.createAlignedTimeseries(
        device, measurements, dataTypes, encodings, compressionTypes, null);
  }

  private boolean dataTypesValid(List<TSDataType> dataTypes) {
    return dataTypes.size() == measurements.size();
  }

  

  @Override
  public void deleteAlignedTimeSeries()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteTimeseries(paths.stream().map(Path::getFullPath).toList());
  }

  @Override
  public void insert(long timestamp, List<TSDataType> dataTypes, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.insertAlignedRecord(device, timestamp, measurements, dataTypes, values);
  }

  @Override
  public List<List<Object>> select(List<Path> first, String timeFilter, String valueFilter) {
    String sql = prepareSelectSql(measurements, device, timeFilter, valueFilter);
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      return wrapperToResult(wrapper);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new ArrayList<>();
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private List<List<Object>> wrapperToResult(SessionDataSetWrapper wrapper)
      throws IoTDBConnectionException, StatementExecutionException {
    List<List<Object>> result = new ArrayList<>();
    SessionDataSet.DataIterator iterator = wrapper.iterator();
    int rowSize = measurements.size();
    while (iterator.next()) {
      List<Object> row = new ArrayList<>();
      // starting from 2, ignore the timestamp
      for (int i = 2; i <= rowSize + 1; i++) {
        row.add(iterator.getObject(i));
      }
      result.add(row);
    }
    return result;
  }
}
