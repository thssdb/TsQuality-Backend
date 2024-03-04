package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.BooleanTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class BooleanTVList extends TVList {
  protected List<BooleanTVPair> pairs = new ArrayList<>();

  BooleanTVList() {
    dataType = TSDataType.BOOLEAN;
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
    return pairs.get(i).getBoolean();
  }

  @Override
  public void putBooleanPair(BooleanTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putBooleanPair(long timestamp, boolean value) {
    BooleanTVPair pair = (BooleanTVPair) TVPairFactory.createTVPair(TSDataType.BOOLEAN);
    pair.setTimestamp(timestamp);
    pair.setBoolean(value);
    pairs.add(pair);
  }

  @Override
  public BooleanTVPair getBooleanPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
