package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.DoubleTVPair;
import java.util.ArrayList;
import java.util.List;

public class DoubleTVList extends TVList {
  protected List<DoubleTVPair> pairs = new ArrayList<>();

  @Override
  public void putDoublePair(DoubleTVPair pair) {
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
