package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class IntTVPair extends TVPair {
  protected int value = 0;

  IntTVPair() {}

  @Override
  public void setInt(int value) {
    this.value = value;
  }

  @Override
  public int getInt() {
    return value;
  }
}
