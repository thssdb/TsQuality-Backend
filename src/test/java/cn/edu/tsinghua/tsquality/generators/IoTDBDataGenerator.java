package cn.edu.tsinghua.tsquality.generators;

import lombok.Getter;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class IoTDBDataGenerator {
  public String[] pathStrings;

  @Getter
  private final Path[] paths;

  private final Session session;

  public IoTDBDataGenerator(@Value("${iotdb.test.data.paths}") String[] pathStrings) throws IoTDBConnectionException {
    this.pathStrings = pathStrings;
    this.session = new Session.Builder().build();
    this.paths = Arrays.stream(pathStrings).map(x -> new Path(x, true)).toArray(Path[]::new);
  }

  public void generateTimestampAnomalyData(int size) throws IoTDBConnectionException {
    try {
      session.open();
      for (Path path: paths) {
        createTimeseriesIfNotExists(path);
        insertTimestampAnomalyDataForTimeseries(path, size);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      session.close();
    }
  }

  public void deleteAll() throws IoTDBConnectionException {
    try {
      session.open();
      for (Path path: paths) {
        String fullPath = path.getFullPath();
        if (session.checkTimeseriesExists(fullPath)) {
          session.deleteTimeseries(fullPath);
        }
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      session.close();
    }
  }

  private void createTimeseriesIfNotExists(Path path) throws IoTDBConnectionException, StatementExecutionException {
    if (!session.checkTimeseriesExists(path.getFullPath())) {
      session.createTimeseries(path.getFullPath(), TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
    }
  }

  private void insertTimestampAnomalyDataForTimeseries(Path path, int size)
      throws IoTDBConnectionException, StatementExecutionException {
    long[] timestamps = TimestampGenerator.timestampsWithHalfAnomalies(size);
    Double[] values = ValueGenerator.generateZeroDoubleValues(size);
    Tablet tablet = populateTablet(path, timestamps, values);
    session.insertTablet(tablet);
  }

  private long[] timestampsWithAnomalies(int size) {
    long[] timestamps = new long[size];
    for (int i = 0; i < size; i++) {
      timestamps[i] = i;
    }
    return timestamps;
  }

  private Tablet populateTablet(Path path, long[] timestamps, Object[] values) {
    Tablet tablet = new Tablet(
        path.getDevice(), List.of(new MeasurementSchema(path.getMeasurement(), TSDataType.DOUBLE, TSEncoding.RLE))
    );
    tablet.rowSize = timestamps.length;
    for (int i = 0; i < timestamps.length; i++) {
      tablet.addTimestamp(i, timestamps[i]);
      tablet.addValue(path.getMeasurement(), i, values[i]);
    }
    return tablet;
  }
}
