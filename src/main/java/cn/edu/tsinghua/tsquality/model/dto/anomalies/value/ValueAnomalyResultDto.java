package cn.edu.tsinghua.tsquality.model.dto.anomalies.value;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class ValueAnomalyResultDto {
  private List<ValueRepairedDataPointDto> data;

  public ValueAnomalyResultDto(TVList original, TVList repaired) throws RuntimeException {
    int size = original.size();
    if (size != repaired.size()) {
      throw new RuntimeException(
          String.format(
              "size of original data(%d) and repaired data(%d) does not match",
              size, repaired.size()));
    }
    data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      data.add(ValueRepairedDataPointDto.from(original.getPair(i), repaired.getPair(i)));
    }
  }
}
