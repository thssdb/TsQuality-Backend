package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.EmptyTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVListFactory;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class RepositoryImpl implements Repository {
  private final Path path;
  private final TSDataType dataType;
  private final Session session;

  public RepositoryImpl(Session session, Path path, TSDataType dataType)
      throws IoTDBConnectionException {
    this.path = path;
    this.dataType = dataType;
    this.session = session;
    this.session.open();
  }

  public RepositoryImpl(Session session, String path, TSDataType dataType)
      throws IoTDBConnectionException {
    this(session, new Path(path, true), dataType);
  }

  @Override
  public void createTimeSeries() {
    try {
      if (!session.checkTimeseriesExists(path.getFullPath())) {
        session.createTimeseries(
            path.getFullPath(), dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteTimeSeries() {
    try {
      if (session.checkTimeseriesExists(path.getFullPath())) {
        session.deleteTimeseries(path.getFullPath());
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TVList select(String timeFilter, String valueFilter) {
    String sql = prepareSelectSql(timeFilter, valueFilter);
    try {
      SessionDataSet dataset = executeSelectSql(sql);
      return datasetToTVList(dataset);
    } catch (Exception ignored) {
      return new EmptyTVList();
    }
  }

  @Override
  public void insert(TVList tvList) {
    Tablet tablet = prepareTablet(tvList);
    try {
      insertTablet(tablet);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private String prepareSelectSql(String timeFilter, String valueFilter) {
    String sql = String.format("select %s from %s", path.getMeasurement(), path.getDevice());
    boolean timeFilterValid = timeFilter != null && !timeFilter.isEmpty();
    boolean valueFilterValid = valueFilter != null && !valueFilter.isEmpty();
    if (timeFilterValid && valueFilterValid) {
      sql += String.format(" where %s and %s", timeFilter, valueFilter);
    } else if (timeFilterValid) {
      sql += String.format(" where %s", timeFilter);
    } else if (valueFilterValid) {
      sql += String.format(" where %s", valueFilter);
    }
    return sql;
  }

  private SessionDataSet executeSelectSql(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    return session.executeQueryStatement(sql);
  }

  private TVList datasetToTVList(SessionDataSet dataset)
      throws IoTDBConnectionException, StatementExecutionException {
    TVList tvList = newTVList(dataType);
    populateDataSetToTVList(dataset, tvList);
    return tvList;
  }

  private TVList newTVList(TSDataType dataType) {
    return TVListFactory.createTVList(dataType);
  }

  private void populateDataSetToTVList(SessionDataSet dataset, TVList tvList)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet.DataIterator iterator = dataset.iterator();
    while (iterator.next()) {
      long timestamp = iterator.getLong(1);
      switch (dataType) {
        case BOOLEAN -> tvList.putBooleanPair(timestamp, iterator.getBoolean(2));
        case INT32 -> tvList.putIntPair(timestamp, iterator.getInt(2));
        case INT64 -> tvList.putLongPair(timestamp, iterator.getLong(2));
        case FLOAT -> tvList.putFloatPair(timestamp, iterator.getFloat(2));
        case DOUBLE -> tvList.putDoublePair(timestamp, iterator.getDouble(2));
        case TEXT -> tvList.putTextPair(timestamp, iterator.getString(2));
        default -> throw new IllegalArgumentException("Unsupported data type");
      }
    }
  }

  private Tablet prepareTablet(TVList tvList) {
    List<MeasurementSchema> schemas = prepareSchemas(tvList);
    return new Tablet(path.getDevice(), schemas);
  }

  private List<MeasurementSchema> prepareSchemas(TVList tvList) {
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema(path.getMeasurement(), tvList.getDataType()));
    return schemas;
  }

  private void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertTablet(tablet);
  }
}
