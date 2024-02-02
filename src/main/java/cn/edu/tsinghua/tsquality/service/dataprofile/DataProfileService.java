package cn.edu.tsinghua.tsquality.service.dataprofile;

import cn.edu.tsinghua.tsquality.model.dto.IoTDBDataProfile;
import cn.edu.tsinghua.tsquality.model.dto.IoTDBSeriesOverview;
import java.util.List;

public interface DataProfileService {
  Long getNumTimeSeries();

  Long getNumDevices();

  Long getNumDatabases();

  IoTDBDataProfile getOverallDataProfile();

  List<IoTDBSeriesOverview> getTimeSeriesOverview();

  List<IoTDBSeriesOverview> getDeviceOverview(String path);

  List<IoTDBSeriesOverview> getDatabaseOverview(String path);
}
