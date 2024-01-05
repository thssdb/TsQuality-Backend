package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TVPairFactory {
  public static TVPair createTVPair(TSDataType dataType) {
    return switch (dataType) {
      case BOOLEAN -> new BooleanTVPair();
      case INT32 -> new IntTVPair();
      case INT64 -> new LongTVPair();
      case FLOAT -> new FloatTVPair();
      case DOUBLE -> new DoubleTVPair();
      case TEXT -> new BinaryTVPair();
      default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
    };
  }
}
