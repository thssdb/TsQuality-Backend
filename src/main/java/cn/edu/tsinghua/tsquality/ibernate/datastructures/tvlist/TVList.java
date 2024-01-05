package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.*;

public abstract class TVList {
  public void putBooleanPair(BooleanTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putIntPair(IntTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putLongPair(LongTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putFloatPair(FloatTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putDoublePair(DoubleTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void putBinaryPair(BinaryTVPair pair) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public BooleanTVPair getBooleanPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TVPair getIntPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TVPair getLongPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TVPair getFloatPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TVPair getDoublePair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public TVPair getBinaryPair(int index) {
    throw new UnsupportedOperationException("DataType not consistent");
  }
}
