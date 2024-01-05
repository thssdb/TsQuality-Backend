package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;


import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TVListFactory {
  public static TVList createTVList(TSDataType dataType) {
    return switch (dataType) {
      case BOOLEAN -> new BooleanTVList();
      case INT32 -> new IntTVList();
      case INT64 -> new LongTVList();
      case FLOAT -> new FloatTVList();
      case DOUBLE -> new DoubleTVList();
      case TEXT -> new BinaryTVList();
      default -> throw new IllegalStateException("Unexpected value: " + dataType);
    };
  }
}
