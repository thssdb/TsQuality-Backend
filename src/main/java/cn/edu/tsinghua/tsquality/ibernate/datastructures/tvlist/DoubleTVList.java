package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.DoubleTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DoubleTVList extends TVList {
  protected List<DoubleTVPair> pairs = new ArrayList<>();

  DoubleTVList() {}

  @Override
  public TSDataType getDataType() {
    return TSDataType.DOUBLE;
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
    return pairs.get(i).getDouble();
  }

  @Override
  public void putDoublePair(DoubleTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putDoublePair(long timestamp, double value) {
    DoubleTVPair pair = (DoubleTVPair) TVPairFactory.createTVPair(TSDataType.DOUBLE);
    pair.setTimestamp(timestamp);
    pair.setDouble(value);
    pairs.add(pair);
  }

  @Override
  public DoubleTVPair getDoublePair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
