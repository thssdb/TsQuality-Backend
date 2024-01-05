package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.BooleanTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class BooleanTVList extends TVList {
  protected List<BooleanTVPair> pairs = new ArrayList<>();

  BooleanTVList() {}

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
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
