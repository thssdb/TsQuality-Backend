package ibernate.datacreators;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.IntTVList;
import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVListFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class IntTVListCreator {
  public static IntTVList create(int count) {
    IntTVList tvList = (IntTVList) TVListFactory.createTVList(TSDataType.INT32);
    for (int i = 0; i < count; i++) {
      tvList.putIntPair(i, i);
    }
    return tvList;
  }
}
