package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.FloatTVPair;
import java.util.ArrayList;
import java.util.List;

public class FloatTVList extends TVList {
  protected List<FloatTVPair> pairs = new ArrayList<>();

  @Override
  public void putFloatPair(FloatTVPair pair) {
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
