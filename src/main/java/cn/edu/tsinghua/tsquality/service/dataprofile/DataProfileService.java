package cn.edu.tsinghua.tsquality.service.dataprofile;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBDataProfile;
import cn.edu.tsinghua.tsquality.model.dto.OverviewResponseDto;

public interface DataProfileService {
  Long getNumTimeSeries();

  Long getNumDevices();

  Long getNumDatabases();

  IoTDBDataProfile getOverallDataProfile();

  OverviewResponseDto getTimeSeriesOverview(int pageIndex, int pageSize);

  OverviewResponseDto getDeviceOverview(int pageIndex, int pageSize);

  OverviewResponseDto getDatabaseOverview(int pageIndex, int pageSize);
}
