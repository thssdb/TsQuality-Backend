package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class EmptyTVList extends TVList {
  @Override
  public TSDataType getDataType() {
    throw new UnsupportedOperationException();
  }
}
