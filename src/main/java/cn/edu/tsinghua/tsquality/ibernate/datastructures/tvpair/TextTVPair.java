package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

public class TextTVPair extends TVPair {
  protected String value = "";

  TextTVPair() {}

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public String getText() {
    return value;
  }

  @Override
  public void setText(String value) {
    this.value = value;
  }
}
