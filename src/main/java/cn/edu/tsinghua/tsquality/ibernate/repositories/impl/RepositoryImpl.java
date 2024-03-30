package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.EmptyTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVListFactory;
import cn.edu.tsinghua.tsquality.ibernate.repositories.Repository;
import cn.edu.tsinghua.tsquality.ibernate.udfs.AbstractUDF;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class RepositoryImpl extends BaseRepository implements Repository {
  private final Path path;
  private final SessionPool sessionPool;
  private TSDataType dataType;

  public RepositoryImpl(SessionPool sessionPool, Path path) {
    this.path = path;
    this.sessionPool = sessionPool;
  }

  public RepositoryImpl(SessionPool sessionPool, String path) {
    this.path = new Path(path, true);
    this.sessionPool = sessionPool;
  }

  @Override
  public void createTimeSeries(TSDataType dataType) {
    try {
      if (!sessionPool.checkTimeseriesExists(path.getFullPath())) {
        sessionPool.createTimeseries(
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
      if (sessionPool.checkTimeseriesExists(fullPath)) {
        sessionPool.deleteTimeseries(fullPath);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TVList select(AbstractUDF udf, String timeFilter, String valueFilter) {
    setDataTypeIfNeeded();
    String sql = prepareUdfSelectSql(udf, timeFilter, valueFilter, 0);
    try {
      SessionDataSetWrapper wrapper = executeSelectSql(sql);
      return datasetToTVList(wrapper);
    } catch (Exception ignored) {
      return new EmptyTVList();
    }
  }

  private String prepareUdfSelectSql(AbstractUDF udf, String timeFilter, String valueFilter, long limit) {
    String selectClause = udf.getSql(path);
    String whereClause = prepareWhereClause(timeFilter, valueFilter);
    String limitClause = prepareLimitClause(limit);
    return selectClause + whereClause + limitClause;
  }


  @Override
  public TVList select(long limit) {
    setDataTypeIfNeeded();
    String sql = prepareSelectSqlWithLimit(path.getMeasurement(), path.getDevice(), limit);
    try {
      SessionDataSetWrapper wrapper = executeSelectSql(sql);
      return datasetToTVList(wrapper);
    } catch (Exception ignored) {
      return new EmptyTVList();
    }
  }

  private void setDataTypeIfNeeded() {
    if (dataType == null) {
      dataType = getDataType();
    }
  }

  @Override
  public TVList select(String timeFilter, String valueFilter) {
    setDataTypeIfNeeded();
    SessionDataSetWrapper wrapper = null;
    String sql =
        prepareSelectSql(List.of(path.getMeasurement()), path.getDevice(), timeFilter, valueFilter);
    try {
      wrapper = executeSelectSql(sql);
      return datasetToTVList(wrapper);
    } catch (Exception ignored) {
      return new EmptyTVList();
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private TSDataType getDataType() {
    try {
      String sql = String.format("show timeseries %s", path.getFullPath());
      SessionDataSet.DataIterator iterator = sessionPool.executeQueryStatement(sql).iterator();
      if (!iterator.next()) {
        throw new RuntimeException("No such timeseries");
      }
      String dataType = iterator.getString("DataType");
      return TSDataType.valueOf(dataType);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private SessionDataSetWrapper executeSelectSql(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    return sessionPool.executeQueryStatement(sql);
  }

  private TVList datasetToTVList(SessionDataSetWrapper wrapper)
      throws IoTDBConnectionException, StatementExecutionException {
    TVList tvList = newTVList(dataType);
    populateDataSetToTVList(wrapper, tvList);
    return tvList;
  }

  private TVList newTVList(TSDataType dataType) {
    return TVListFactory.createTVList(dataType);
  }

  private void populateDataSetToTVList(SessionDataSetWrapper wrapper, TVList tvList)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet.DataIterator iterator = wrapper.iterator();
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
    sessionPool.insertTablet(tablet);
  }

  public void flush() throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.executeNonQueryStatement("flush");
  }
}
