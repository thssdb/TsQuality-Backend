package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.BinaryTVPair;

import java.util.ArrayList;
import java.util.List;

public class BinaryTVList extends TVList {
  protected List<BinaryTVPair> pairs = new ArrayList<>();

  @Override
  public void putBinaryPair(BinaryTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public BinaryTVPair getBinaryPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
