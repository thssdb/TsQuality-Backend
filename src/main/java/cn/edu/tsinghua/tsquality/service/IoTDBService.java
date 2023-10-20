package cn.edu.tsinghua.tsquality.service;

import cn.edu.tsinghua.tsquality.mapper.IoTDBConfigMapper;
import cn.edu.tsinghua.tsquality.model.entity.IoTDBConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class IoTDBService {
    private static final String SQL_QUERY_NUMS_TIME_SERIES = "COUNT TIMESERIES";
    private static final String SQL_QUERY_NUMS_DEVICES = "COUNT DEVICES";
    private static final String SQL_QUERY_NUMS_DATABASES = "COUNT DATABASES";

    @Autowired
    IoTDBConfigMapper ioTDBConfigMapper;

    private static Session buildSession(IoTDBConfig ioTDBConfig) {
        Session session;
        try {
            session = new Session.Builder()
                    .host(ioTDBConfig.getHost())
                    .port(ioTDBConfig.getPort())
                    .username(ioTDBConfig.getUsername())
                    .password(ioTDBConfig.getPassword())
                    .build();
        } catch (IllegalArgumentException e) {
            return null;
        }
        return session;
    }

    public long getCountResult(int iotdbConfigID, String sql) {
        IoTDBConfig ioTDBConfig = ioTDBConfigMapper.getWithPasswordById(iotdbConfigID);
        try (Session session = buildSession(ioTDBConfig)) {
            if (session == null) {
                return 0;
            }
            session.open();
            SessionDataSet dataSet = session.executeQueryStatement(sql);
            SessionDataSet.DataIterator iterator = dataSet.iterator();
            if (iterator.next()) {
                return iterator.getLong(1);
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            System.out.println(e.getMessage());
            return 0;
        }
        return 0;
    }

    public long getNumsTimeSeries(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_TIME_SERIES);
    }

    public long getNumsDevices(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DEVICES);
    }

    public long getNumsDatabases(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DATABASES);
    }

    public long getNumsStorageGroups(int iotdbConfigId) {
        return getCountResult(iotdbConfigId, SQL_QUERY_NUMS_DATABASES);
    }
}
