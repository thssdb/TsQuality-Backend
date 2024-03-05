package cn.edu.tsinghua.tsquality.service.dataprofile.impl;

import cn.edu.tsinghua.tsquality.common.DataQualityCalculationUtil;
import cn.edu.tsinghua.tsquality.ibernate.clients.Client;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBDataProfile;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesOverview;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBSeriesStat;
import cn.edu.tsinghua.tsquality.service.dataprofile.DataProfileService;
import cn.edu.tsinghua.tsquality.storage.MetadataStorageEngine;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class DataProfileServiceImpl implements DataProfileService {
  private final MetadataStorageEngine storageEngine;
  private final Client iotdbClient;

  public DataProfileServiceImpl(MetadataStorageEngine storageEngine, Client iotdbClient) {
    this.storageEngine = storageEngine;
    this.iotdbClient = iotdbClient;
  }

  @Override
  public Long getNumTimeSeries() {
    return iotdbClient.countTimeSeries();
  }

  @Override
  public Long getNumDevices() {
    return iotdbClient.countDevices();
  }

  @Override
  public Long getNumDatabases() {
    return iotdbClient.countDatabases();
  }

  @Override
  public IoTDBDataProfile getOverallDataProfile() {
    IoTDBSeriesStat stat = storageEngine.selectAllStats();
    double completeness = DataQualityCalculationUtil.calculateCompleteness(stat);
    double consistency = DataQualityCalculationUtil.calculateConsistency(stat);
    double timeliness = DataQualityCalculationUtil.calculateTimeliness(stat);
    double validity = DataQualityCalculationUtil.calculateValidity(stat);
    return IoTDBDataProfile.builder()
        .numDataPoints(stat.getCount())
        .numTimeSeries(getNumTimeSeries())
        .numDevices(getNumDevices())
        .numDatabases(getNumDatabases())
        .completeness(completeness)
        .consistency(consistency)
        .timeliness(timeliness)
        .validity(validity)
        .build();
  }

  @Override
  public List<IoTDBSeriesOverview> getTimeSeriesOverview() {
    return storageEngine.selectSeriesStats(null).stream().map(IoTDBSeriesOverview::new).toList();
  }

  @Override
  public List<IoTDBSeriesOverview> getDeviceOverview(String path) {
    return storageEngine.selectDeviceStats(path).stream().map(IoTDBSeriesOverview::new).toList();
  }

  @Override
  public List<IoTDBSeriesOverview> getDatabaseOverview(String path) {
    return storageEngine.selectDatabaseStats(path).stream().map(IoTDBSeriesOverview::new).toList();
  }
}
