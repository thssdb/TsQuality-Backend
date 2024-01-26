package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class FloatTVPair extends TVPair {
  protected float value = 0.0f;

  FloatTVPair() {}

  @Override
  public float getFloat() {
    return value;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setFloat(float value) {
    this.value = value;
  }
}
