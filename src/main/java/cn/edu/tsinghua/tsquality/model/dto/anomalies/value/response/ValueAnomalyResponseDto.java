package cn.edu.tsinghua.tsquality.model.dto.anomalies.value.response;

import cn.edu.tsinghua.tsquality.ibernate.datastructures.tvlist.TVList;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ValueAnomalyResponseDto {
  private List<ValueRepairedDataPointDto> data;

  public ValueAnomalyResponseDto(TVList original, TVList repaired) throws RuntimeException {
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
