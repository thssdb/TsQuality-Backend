package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class LongTVPair extends TVPair {
  protected long value = 0L;

  LongTVPair() {}

  @Override
  public void setLong(long value) {
    this.value = value;
  }

  @Override
  public long getLong() {
    return value;
  }
}
