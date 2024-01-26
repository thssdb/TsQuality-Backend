package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class BooleanTVPair extends TVPair {
  protected boolean value = false;

  BooleanTVPair() {}

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public boolean getBoolean() {
    return value;
  }

  @Override
  public void setBoolean(boolean value) {
    this.value = value;
  }
}
