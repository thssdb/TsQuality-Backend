package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.EmptyTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVListFactory;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.udfs.AbstractUDF;
import java.util.ArrayList;
import java.util.List;
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

public class RepositoryImpl implements Repository {
  private final Path path;
  private final Session session;
  private TSDataType dataType;

  public RepositoryImpl(Session session, Path path) throws IoTDBConnectionException {
    this.path = path;
    this.session = session;
    this.session.open();
  }

  public RepositoryImpl(Session session, String path) throws IoTDBConnectionException {
    this(session, new Path(path, true));
  }

  @Override
  public void createTimeSeries(TSDataType dataType) {
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
      String fullPath = path.getFullPath();
      if (session.checkTimeseriesExists(fullPath)) {
        session.deleteTimeseries(fullPath);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TVList select(AbstractUDF udf, String timeFilter, String valueFilter) {
    setDataTypeIfNeeded();
    String sql = prepareSelectSql(udf, timeFilter, valueFilter);
    try {
      SessionDataSet dataset = executeSelectSql(sql);
      return datasetToTVList(dataset);
    } catch (Exception ignored) {
      return new EmptyTVList();
    }
  }

  private void setDataTypeIfNeeded() {
    if (dataType == null) {
      dataType = getDataType();
    }
  }

  private String prepareSelectSql(AbstractUDF udf, String timeFilter, String valueFilter) {
    String selectClause = udf.getSql(path);
    String whereClause = prepareWhereClause(timeFilter, valueFilter);
    return selectClause + whereClause;
  }

  private String prepareWhereClause(String timeFilter, String valueFilter) {
    boolean timeFilterValid = timeFilter != null && !timeFilter.isEmpty();
    boolean valueFilterValid = valueFilter != null && !valueFilter.isEmpty();
    if (timeFilterValid && valueFilterValid) {
      return String.format(" where %s and %s", timeFilter, valueFilter);
    } else if (timeFilterValid) {
      return String.format(" where %s", timeFilter);
    } else if (valueFilterValid) {
      return String.format(" where %s", valueFilter);
    } else {
      return "";
    }
  }

  @Override
  public TVList select(String timeFilter, String valueFilter) {
    setDataTypeIfNeeded();
    String sql = prepareSelectSql(timeFilter, valueFilter);
    try {
      SessionDataSet dataset = executeSelectSql(sql);
      return datasetToTVList(dataset);
    } catch (Exception ignored) {
      return new EmptyTVList();
    }
  }

  private TSDataType getDataType() {
    try {
      String sql = String.format("show timeseries %s", path.getFullPath());
      SessionDataSet.DataIterator iterator = session.executeQueryStatement(sql).iterator();
      if (!iterator.next()) {
        throw new RuntimeException("No such timeseries");
      }
      String dataType = iterator.getString("DataType");
      return TSDataType.valueOf(dataType);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private String prepareSelectSql(String timeFilter, String valueFilter) {
    String selectClause =
        String.format("select %s from %s", path.getMeasurement(), path.getDevice());
    String whereClause = prepareWhereClause(timeFilter, valueFilter);
    return selectClause + whereClause;
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
      switch (tvList.getDataType()) {
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

  @Override
  public void insert(TVList tvList) {
    Tablet tablet = prepareTablet(tvList);
    try {
      insertTablet(tablet);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private Tablet prepareTablet(TVList tvList) {
    List<MeasurementSchema> schemas = prepareSchemas(tvList);
    Tablet tablet = new Tablet(path.getDevice(), schemas);
    populateTVListToTablet(tvList, tablet);
    return tablet;
  }

  private List<MeasurementSchema> prepareSchemas(TVList tvList) {
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema(path.getMeasurement(), tvList.getDataType()));
    return schemas;
  }

  private void populateTVListToTablet(TVList tvList, Tablet tablet) {
    tablet.rowSize = tvList.size();
    String measurement = path.getMeasurement();
    for (int i = 0; i < tvList.size(); i++) {
      tablet.addTimestamp(i, tvList.getTimestamp(i));
      switch (tvList.getDataType()) {
        case BOOLEAN -> tablet.addValue(measurement, i, tvList.getBooleanPair(i).getBoolean());
        case INT32 -> tablet.addValue(measurement, i, tvList.getIntPair(i).getInt());
        case INT64 -> tablet.addValue(measurement, i, tvList.getLongPair(i).getLong());
        case FLOAT -> tablet.addValue(measurement, i, tvList.getFloatPair(i).getFloat());
        case DOUBLE -> tablet.addValue(measurement, i, tvList.getDoublePair(i).getDouble());
        case TEXT -> tablet.addValue(measurement, i, tvList.getTextPair(i).getText());
        default -> throw new IllegalArgumentException("Unsupported data type");
      }
    }
  }

  private void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertTablet(tablet);
  }

  public void flush() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("flush");
  }
}
