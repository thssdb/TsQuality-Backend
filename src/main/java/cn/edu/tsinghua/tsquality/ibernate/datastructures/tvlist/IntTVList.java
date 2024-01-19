package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.IntTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class IntTVList extends TVList {
  protected List<IntTVPair> pairs = new ArrayList<>();

  IntTVList() {}

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT32;
  }

  @Override
  public int size() {
    return pairs.size();
  }

  @Override
  public long getTimestamp(int i) {
    if (i >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return pairs.get(i).getTimestamp();
  }

  @Override
  public Object getValue(int i) {
    if (i >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return pairs.get(i).getInt();
  }

  @Override
  public void putIntPair(IntTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putIntPair(long timestamp, int value) {
    IntTVPair pair = (IntTVPair) TVPairFactory.createTVPair(TSDataType.INT32);
    pair.setTimestamp(timestamp);
    pair.setInt(value);
    pairs.add(pair);
  }

  @Override
  public IntTVPair getIntPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
