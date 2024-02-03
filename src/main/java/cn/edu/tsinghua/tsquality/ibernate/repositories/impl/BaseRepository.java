package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import java.util.List;
import org.apache.logging.log4j.util.Strings;

public class BaseRepository {
  protected String prepareSelectSql(
      List<String> measurements, String device, String timeFilter, String valueFilter) {
    String selectClause =
        String.format("select %s from %s", Strings.join(measurements, ','), device);
    String whereClause = prepareWhereClause(timeFilter, valueFilter);
    return selectClause + whereClause;
  }

  protected String prepareWhereClause(String timeFilter, String valueFilter) {
    boolean timeFilterValid = timeFilter != null && !timeFilter.isEmpty();
    boolean valueFilterValid = valueFilter != null && !valueFilter.isEmpty();
    if (timeFilterValid && valueFilterValid) {
      return String.format(" where %s and %s", timeFilter, valueFilter);
    } else if (timeFilterValid) {
      return String.format(" where %s", timeFilter);
    } else if (valueFilterValid) {
      return String.format(" where %s", valueFilter);
    } else {
      return "";
    }
  }
}
