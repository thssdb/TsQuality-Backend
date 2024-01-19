package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.LongTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class LongTVList extends TVList {
  protected List<LongTVPair> pairs = new ArrayList<>();

  LongTVList() {}

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT64;
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
    return pairs.get(i).getLong();
  }

  @Override
  public void putLongPair(LongTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putLongPair(long timestamp, long value) {
    LongTVPair pair = (LongTVPair) TVPairFactory.createTVPair(TSDataType.INT64);
    pair.setTimestamp(timestamp);
    pair.setLong(value);
    pairs.add(pair);
  }

  @Override
  public LongTVPair getLongPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
