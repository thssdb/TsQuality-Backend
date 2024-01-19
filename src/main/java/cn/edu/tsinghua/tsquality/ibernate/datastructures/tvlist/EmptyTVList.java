package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class EmptyTVList extends TVList {
  @Override
  public TSDataType getDataType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimestamp(int i) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getValue(int i) {
    throw new UnsupportedOperationException();
  }
}
