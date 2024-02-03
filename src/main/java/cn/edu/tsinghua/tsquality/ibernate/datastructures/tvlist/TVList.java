package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.*;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

@Data
public abstract class TVList {
  protected TSDataType dataType;

  public abstract TSDataType getDataType();

  public abstract int size();

  public abstract long getTimestamp(int i);

  public abstract Object getValue(int i);

  public List<TVPair> getPairs() {
    int size = size();
    List<TVPair> pairs = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      pairs.add(getPair(i));
    }
    return pairs;
  }

  public TVPair getPair(int i) {
    return switch (dataType) {
      case BOOLEAN -> getBooleanPair(i);
      case INT32 -> getIntPair(i);
      case INT64 -> getLongPair(i);
      case FLOAT -> getFloatPair(i);
      case DOUBLE -> getDoublePair(i);
      case TEXT -> getTextPair(i);
      default -> throw new UnsupportedOperationException("DataType not consistent");
    };
  }

  public void putBooleanPair(BooleanTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putBooleanPair(long timestamp, boolean value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putIntPair(IntTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putIntPair(long timestamp, int value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putLongPair(LongTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putLongPair(long timestamp, long value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putFloatPair(FloatTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putFloatPair(long timestamp, float value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putDoublePair(DoubleTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putDoublePair(long timestamp, double value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putTextPair(TextTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putTextPair(long timestamp, String value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public BooleanTVPair getBooleanPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public IntTVPair getIntPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public LongTVPair getLongPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public FloatTVPair getFloatPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public DoubleTVPair getDoublePair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TextTVPair getTextPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }
}
