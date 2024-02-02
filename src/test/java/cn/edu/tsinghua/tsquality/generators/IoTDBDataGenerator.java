package cn.edu.tsinghua.tsquality.generators;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IoTDBDataGenerator {
  public static final int SERIES_COUNT = 3;
  public static final int DEVICE_COUNT = 1;
  public static final int DATABASE_COUNT = 1;

  @Autowired private TimestampGenerator timestampGenerator;
  @Autowired private ValueGenerator valueGenerator;

  @Getter
  @Setter
  @Value("${pre-aggregation.data-dir}")
  private String sequenceDataDir;

  @Getter private static final String DATABASE_NAME = "root.tsquality";
  @Getter private static final List<Path> paths;

  static {
    paths = new ArrayList<>();
    for (int i = 1; i <= SERIES_COUNT; i++) {
      String path = String.format("%s.d1.s%d", DATABASE_NAME, i);
      paths.add(new Path(path, true));
    }
  }

  private final Session session;

  public IoTDBDataGenerator() {
    this.session = new Session.Builder().build();
  }

  public void generateTimestampAnomalyData(int size)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.open();
      for (Path path : paths) {
        createTimeseriesIfNotExists(path);
        Tablet tablet = generateTabletWithTimestampAnomalies(path, size);
        insertTablet(tablet);
      }
    } finally {
      session.close();
    }
  }

  private void createTimeseriesIfNotExists(Path path)
      throws IoTDBConnectionException, StatementExecutionException {
    if (!session.checkTimeseriesExists(path.getFullPath())) {
      session.createTimeseries(
          path.getFullPath(), TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
    }
  }

  private Tablet generateTabletWithTimestampAnomalies(Path path, int size) {
    long[] timestamps = timestampGenerator.timestampsWithHalfAnomalies(size);
    Double[] values = valueGenerator.zeroDoubleValues(size);
    return populateTablet(path, timestamps, values);
  }

  private void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    session.insertTablet(tablet);
  }

  private Tablet populateTablet(Path path, long[] timestamps, Object[] values) {
    List<MeasurementSchema> schemas =
        List.of(new MeasurementSchema(path.getMeasurement(), TSDataType.DOUBLE, TSEncoding.RLE));
    Tablet tablet = new Tablet(path.getDevice(), schemas);
    tablet.rowSize = timestamps.length;
    for (int i = 0; i < timestamps.length; i++) {
      tablet.addTimestamp(i, timestamps[i]);
      tablet.addValue(path.getMeasurement(), i, values[i]);
    }
    return tablet;
  }

  public void deleteAll() throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.open();
      for (Path path : paths) {
        String fullPath = path.getFullPath();
        if (session.checkTimeseriesExists(fullPath)) {
          session.deleteTimeseries(fullPath);
        }
      }
    } finally {
      session.close();
    }
  }

  public void generateData(int size) throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.open();
      for (Path path : paths) {
        createTimeseriesIfNotExists(path);
        Tablet tablet = generateTablet(path, size);
        insertTablet(tablet);
      }
    } finally {
      session.close();
    }
  }

  private Tablet generateTablet(Path path, int size) {
    long[] timestamps = timestampGenerator.standardTimestamps(size);
    Double[] values = valueGenerator.linearDoubleValues(size);
    return populateTablet(path, timestamps, values);
  }

  public void flush() throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.open();
      session.executeNonQueryStatement("flush");
    } finally {
      session.close();
    }
  }

  public void deleteDatabase() throws IoTDBConnectionException, StatementExecutionException {
    try {
      if (checkDatabaseExists(DATABASE_NAME)) {
        String sql = "delete database " + DATABASE_NAME;
        session.open();
        session.executeNonQueryStatement(sql);
      }
    } finally {
      session.close();
    }
  }

  public boolean checkDatabaseExists(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      String sql = "show databases " + database;
      session.open();
      SessionDataSet dataSet = session.executeQueryStatement(sql);
      return dataSet.hasNext();
    } finally {
      session.close();
    }
  }
}
