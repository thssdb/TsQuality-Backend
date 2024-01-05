package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class DoubleTVPair extends TVPair {
  protected double value = 0.0;

  DoubleTVPair() {}

  @Override
  public void setDouble(double value) {
    this.value = value;
  }

  @Override
  public double getDouble() {
    return value;
  }
}
