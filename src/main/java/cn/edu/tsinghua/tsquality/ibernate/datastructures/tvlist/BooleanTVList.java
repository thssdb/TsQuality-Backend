package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.BooleanTVPair;
import java.util.ArrayList;
import java.util.List;

public class BooleanTVList extends TVList {
  protected List<BooleanTVPair> pairs = new ArrayList<>();

  @Override
  public void putBooleanPair(BooleanTVPair pair) {
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
