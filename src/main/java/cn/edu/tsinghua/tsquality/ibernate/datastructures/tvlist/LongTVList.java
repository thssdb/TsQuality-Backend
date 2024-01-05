package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.LongTVPair;

import java.util.List;

public class LongTVList extends TVList {
  protected List<LongTVPair> pairs;

  @Override
  public void putLongPair(LongTVPair pair) {
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
