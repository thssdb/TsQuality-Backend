package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.FloatTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class FloatTVList extends TVList {
  protected List<FloatTVPair> pairs = new ArrayList<>();

  FloatTVList() {
    dataType = TSDataType.FLOAT;
  }

  @Override
  public TSDataType getDataType() {
    return dataType;
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
    return pairs.get(i).getFloat();
  }

  @Override
  public void putFloatPair(FloatTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putFloatPair(long timestamp, float value) {
    FloatTVPair pair = (FloatTVPair) TVPairFactory.createTVPair(TSDataType.FLOAT);
    pair.setTimestamp(timestamp);
    pair.setFloat(value);
    pairs.add(pair);
  }

  @Override
  public FloatTVPair getFloatPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
