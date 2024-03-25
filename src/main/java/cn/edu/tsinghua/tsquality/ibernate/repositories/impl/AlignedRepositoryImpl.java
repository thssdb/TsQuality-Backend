package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.repositories.AlignedRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;

@Log4j2
public class AlignedRepositoryImpl extends BaseRepository implements AlignedRepository {
  @Getter protected String device;
  @Getter protected List<String> measurements;
  @Getter protected List<Path> paths;
  protected final SessionPool sessionPool;

  public AlignedRepositoryImpl(SessionPool sessionPool) {
    this.sessionPool = sessionPool;
  }

  public AlignedRepositoryImpl(
      SessionPool sessionPool, @Nonnull String device, @Nonnull List<String> measurements) {
    this.sessionPool = sessionPool;
    this.device = device;
    this.measurements = measurements;
    this.paths = measurements.stream().map(x -> new Path(device + "." + x, true)).toList();
  }

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
    if (alreadyCreated()) {
      return;
    }
    doCreate(dataTypes);
  }

  private boolean alreadyCreated() {
    long size = countTimeSeriesLike(device);
    return size == paths.size();
  }

  private void doCreate(List<TSDataType> dataTypes)
      throws IoTDBConnectionException, StatementExecutionException {
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
  public long count() {
    String sql = countSql(device);
    return executeCountQuery(sql);
  }

  @Override
  public long countTimeSeriesLike(String prefix) {
    String sql = countTimeSeriesLikeSql(prefix);
    return executeCountQuery(sql);
  }

  private long executeCountQuery(String sql) {
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      return wrapperToSelectCountResult(wrapper);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
      return 0;
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private long wrapperToSelectCountResult(SessionDataSetWrapper wrapper)
      throws IoTDBConnectionException, StatementExecutionException {
    long result = 0;
    SessionDataSet.DataIterator iterator = wrapper.iterator();
    if (iterator.next()) {
      result = iterator.getInt(1);
    }
    return result;
  }

  @Override
  public void deleteAlignedTimeSeries()
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.deleteTimeseries(paths.stream().map(Path::getFullPath).toList());
  }

  @Override
  public void insert(long timestamp, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.insertAlignedRecord(
        device, timestamp, measurements, values.stream().map(Object::toString).toList());
  }

  @Override
  public void insert(long timestamp, List<String> measurements, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.insertAlignedRecord(
        device, timestamp, measurements, values.stream().map(Object::toString).toList());
  }

  @Override
  public List<List<Object>> select(String timeFilter, String valueFilter) {
    String sql = prepareSelectSql(measurements, device, timeFilter, valueFilter);
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      return wrapperToSelectResult(wrapper);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
      return new ArrayList<>();
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private List<List<Object>> wrapperToSelectResult(SessionDataSetWrapper wrapper)
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
