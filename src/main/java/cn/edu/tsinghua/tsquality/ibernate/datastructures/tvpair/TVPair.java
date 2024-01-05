package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair;

import lombok.Data;

@Data
public abstract class TVPair {
  protected long timestamp = 0;

  public void setBoolean(boolean value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void setInt(int value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void setLong(long value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void setFloat(float value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void setDouble(double value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public void setText(String value) {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public boolean getBoolean() {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public int getInt() {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public long getLong() {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public float getFloat() {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public double getDouble() {
    throw new UnsupportedOperationException("DataType not consistent");
  }

  public String getText() {
    throw new UnsupportedOperationException("DataType not consistent");
  }
}
