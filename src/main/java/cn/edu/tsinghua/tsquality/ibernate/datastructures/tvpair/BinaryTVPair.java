package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class BinaryTVPair extends TVPair {
  protected byte[] value = new byte[0];

  @Override
  public void setBinary(byte[] value) {
    this.value = value;
  }

  @Override
  public byte[] getBinary() {
    return value;
  }
}
