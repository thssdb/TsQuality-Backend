package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.IntTVPair;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPair;
import java.util.List;

public class IntTVList extends TVList {
  protected List<TVPair> pairs;

  @Override
  public void putIntPair(IntTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public TVPair getIntPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
