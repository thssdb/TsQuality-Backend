package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import org.apache.logging.log4j.util.Strings;

import java.util.List;

public class BaseRepository {
  protected String countSql(String device) {
    return String.format("select count(*) from %s", device);
  }

  protected String countTimeSeriesLikeSql(String prefix) {
    return String.format("count timeseries %s.**", prefix);
  }

  protected String prepareSelectSql(
      List<String> measurements, String device, String timeFilter, String valueFilter) {
    String selectClause =
        String.format("select %s from %s", Strings.join(measurements, ','), device);
    String whereClause = prepareWhereClause(timeFilter, valueFilter);
    return selectClause + whereClause;
  }

  protected String prepareSelectSqlWithLimit(String measurement, String device, long limit) {
    String selectClause = String.format("select %s from %s", measurement, device);
    String limitClause = prepareLimitClause(limit);
    return selectClause + limitClause;
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

  protected String prepareLimitClause(long limit) {
    return limit > 0 ? " limit " + limit : "";
  }
}
