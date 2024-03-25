package cn.edu.tsinghua.tsquality.ibernate.repositories.impl;

import cn.edu.tsinghua.tsquality.common.TimeRange;
import cn.edu.tsinghua.tsquality.ibernate.repositories.StatsAlignedRepository;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.storage.impl.iotdb.StatsTimeSeriesUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public class StatsAlignedRepositoryImpl extends AlignedRepositoryImpl
    implements StatsAlignedRepository {
  public StatsAlignedRepositoryImpl(SessionPool sessionPool, Path originalPath, StatLevel level) throws IoTDBConnectionException, StatementExecutionException {
    super(sessionPool);
    measurements =
        switch (level) {
          case FILE -> {
            device = StatsTimeSeriesUtil.getFileStatsDeviceForPath(originalPath);
            yield StatsTimeSeriesUtil.getFileStatsMeasurementsForPath();
          }
          case CHUNK -> {
            device = StatsTimeSeriesUtil.getChunkStatsDeviceForPath(originalPath);
            yield StatsTimeSeriesUtil.getChunkStatsMeasurementsForPath();
          }
          case PAGE -> {
            device = StatsTimeSeriesUtil.getPageStatsDeviceForPath(originalPath);
            yield StatsTimeSeriesUtil.getPageStatsMeasurementsForPath();
          }
        };
    paths = measurements.stream().map(x -> new Path(device + "." + x, true)).toList();
    createStatsAlignedTimeSeriesIfNotExists(level);
  }

  private void createStatsAlignedTimeSeriesIfNotExists(StatLevel level) throws IoTDBConnectionException, StatementExecutionException {
    switch (level) {
      case FILE:
        createAlignedTimeSeries(StatsTimeSeriesUtil.getFileStatsDataTypes(), StatsTimeSeriesUtil.getFileStatsEncodings());
        break;
      case CHUNK:
        createAlignedTimeSeries(StatsTimeSeriesUtil.getChunkStatsDataTypes(), StatsTimeSeriesUtil.getChunkStatsEncodings());
        break;
      case PAGE:
        createAlignedTimeSeries(StatsTimeSeriesUtil.getPageStatsDataTypes(), StatsTimeSeriesUtil.getPageStatsEncodings());
        break;
    }
  }

  @Override
  public IoTDBSeriesStat selectStats(List<TimeRange> timeRanges) {
    String sql = prepareSelectStatsSql(timeRanges);
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      return new IoTDBSeriesStat(wrapper);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
      return new IoTDBSeriesStat();
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private String prepareSelectStatsSql(List<TimeRange> timeRanges) {
    String timeFilter =
        TimeRange.getStatsTimeFilter(
            timeRanges, StatsTimeSeriesUtil.MIN_TIME, StatsTimeSeriesUtil.MAX_TIME);
    String selectClause =
        String.format(
            "select min_value(%s) as %s, max_value(%s) as %s, sum(%s) as %s, sum(%s) as %s, sum(%s) as %s, "
                + "sum(%s) as %s, sum(%s) as %s, sum(%s) as %s, sum(%s) as %s, sum(%s) as %s, sum(%s) as %s from %s",
            StatsTimeSeriesUtil.MIN_TIME,
            StatsTimeSeriesUtil.MIN_TIME,
            StatsTimeSeriesUtil.MAX_TIME,
            StatsTimeSeriesUtil.MAX_TIME,
            StatsTimeSeriesUtil.COUNT,
            StatsTimeSeriesUtil.COUNT,
            StatsTimeSeriesUtil.MISS_COUNT,
            StatsTimeSeriesUtil.MISS_COUNT,
            StatsTimeSeriesUtil.SPECIAL_COUNT,
            StatsTimeSeriesUtil.SPECIAL_COUNT,
            StatsTimeSeriesUtil.LATE_COUNT,
            StatsTimeSeriesUtil.LATE_COUNT,
            StatsTimeSeriesUtil.REDUNDANT_COUNT,
            StatsTimeSeriesUtil.REDUNDANT_COUNT,
            StatsTimeSeriesUtil.VALUE_COUNT,
            StatsTimeSeriesUtil.VALUE_COUNT,
            StatsTimeSeriesUtil.VARIATION_COUNT,
            StatsTimeSeriesUtil.VARIATION_COUNT,
            StatsTimeSeriesUtil.SPEED_COUNT,
            StatsTimeSeriesUtil.SPEED_COUNT,
            StatsTimeSeriesUtil.ACCELERATION_COUNT,
            StatsTimeSeriesUtil.ACCELERATION_COUNT,
            device);
    String whereClause = prepareWhereClause(timeFilter, null);
    return selectClause + whereClause;
  }

  @Override
  public List<TimeRange> selectTimeRanges(List<TimeRange> timeRanges) {
    String sql = prepareSelectTimeRangesSql(timeRanges);
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = sessionPool.executeQueryStatement(sql);
      return wrapperToTimeRanges(wrapper);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      log.error(e);
      return new ArrayList<>();
    } finally {
      sessionPool.closeResultSet(wrapper);
    }
  }

  private String prepareSelectTimeRangesSql(List<TimeRange> timeRanges) {
    String timeFilter =
        TimeRange.getStatsTimeFilter(
            timeRanges, StatsTimeSeriesUtil.MIN_TIME, StatsTimeSeriesUtil.MAX_TIME);
    String selectClause =
        String.format(
            "select %s as %s, %s as %s from %s",
            StatsTimeSeriesUtil.MIN_TIME,
            StatsTimeSeriesUtil.MIN_TIME,
            StatsTimeSeriesUtil.MAX_TIME,
            StatsTimeSeriesUtil.MAX_TIME,
            device);
    String whereClause = prepareWhereClause(timeFilter, null);
    return selectClause + whereClause;
  }

  private List<TimeRange> wrapperToTimeRanges(SessionDataSetWrapper wrapper)
      throws IoTDBConnectionException, StatementExecutionException {
    List<TimeRange> result = new ArrayList<>();
    SessionDataSet.DataIterator iterator = wrapper.iterator();
    while (iterator.next()) {
      long min = iterator.getLong(StatsTimeSeriesUtil.MIN_TIME);
      long max = iterator.getLong(StatsTimeSeriesUtil.MAX_TIME);
      result.add(new TimeRange(min, max));
    }
    return result;
  }

  public enum StatLevel {
    FILE,
    CHUNK,
    PAGE
  }
}
