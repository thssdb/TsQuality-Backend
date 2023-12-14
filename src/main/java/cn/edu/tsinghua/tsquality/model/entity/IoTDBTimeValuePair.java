package cn.edu.tsinghua.tsquality.model.entity;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

@Data
public class IoTDBTimeValuePair {
    private long timestamp;
    private double value;

    public IoTDBTimeValuePair(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public static List<IoTDBTimeValuePair> buildFromDatasetIterator(
            SessionDataSet.DataIterator iterator) {
        List<IoTDBTimeValuePair> pairs = new ArrayList<>();
        try {
            while (iterator.next()) {
                long timestamp = iterator.getLong(1);
                switch (iterator.getColumnTypeList().get(1)) {
                    case "INT":
                        pairs.add(new IoTDBTimeValuePair(timestamp, iterator.getInt(2)));
                        break;
                    case "INT64":
                        pairs.add(new IoTDBTimeValuePair(timestamp, iterator.getLong(2)));
                        break;
                    case "FLOAT":
                        pairs.add(new IoTDBTimeValuePair(timestamp, iterator.getFloat(2)));
                        break;
                    case "DOUBLE":
                        pairs.add(new IoTDBTimeValuePair(timestamp, iterator.getDouble(2)));
                        break;
                    default:
                        return null;
                }
            }
            return pairs;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            return null;
        }
    }
}
