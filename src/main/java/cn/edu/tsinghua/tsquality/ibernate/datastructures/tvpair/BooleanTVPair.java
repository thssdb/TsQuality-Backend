package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class BooleanTVPair extends TVPair {
  protected boolean value = false;

  BooleanTVPair() {}

  @Override
  public void setBoolean(boolean value) {
    this.value = value;
  }

  @Override
  public boolean getBoolean() {
    return value;
  }
}
