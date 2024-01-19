package cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TVPairFactory;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvpair.TextTVPair;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TextTVList extends TVList {
  protected List<TextTVPair> pairs = new ArrayList<>();

  TextTVList() {}

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  public int size() {
    return pairs.size();
  }

  @Override
  public long getTimestamp(int i) {
    if (i >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return pairs.get(i).getTimestamp();
  }

  @Override
  public Object getValue(int i) {
    if (i >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(i);
    }
    return pairs.get(i).getText();
  }

  @Override
  public void putTextPair(TextTVPair pair) {
    pairs.add(pair);
  }

  @Override
  public void putTextPair(long timestamp, String value) {
    TextTVPair pair = (TextTVPair) TVPairFactory.createTVPair(TSDataType.TEXT);
    pair.setTimestamp(timestamp);
    pair.setText(value);
    pairs.add(pair);
  }

  @Override
  public TextTVPair getTextPair(int index) {
    if (index >= pairs.size()) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return pairs.get(index);
  }
}
