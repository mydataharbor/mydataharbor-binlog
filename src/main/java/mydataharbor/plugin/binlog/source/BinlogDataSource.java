package mydataharbor.plugin.binlog.source;

import lombok.extern.slf4j.Slf4j;
import mydataharbor.ITaskStorage;
import mydataharbor.plugin.binlog.common.exception.BinlogException;
import mydataharbor.plugin.binlog.common.BinlogDataSourceConfig;
import mydataharbor.plugin.binlog.common.Column;
import mydataharbor.datasource.AbstractRateLimitDataSource;
import mydataharbor.exception.TheEndException;
import mydataharbor.setting.BaseSettingContext;
import mydataharbor.threadlocal.TaskStorageThreadLocal;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;

/**
 * @author xulang
 * @date 2023/1/18
 */
@Slf4j
public class BinlogDataSource extends AbstractRateLimitDataSource<BinlogEventWrapper, BaseSettingContext> {

    /**
     * 库->表->字段
     */
    private Map<String, Map<String, List<Column>>> tableColumnInfo;
    /**
     * 数据库库表信息
     * tableId -> 字段定义
     */
    private Map<Long, List<Column>> tableIdColumnInfoMap;

    private BinlogDataSourceConfig binlogDataSourceConfig;

    private BinaryLogClient client;

    private BlockingQueue<Event> eventBlockingQueue;

    private String binlogFileName;

    private FutureTask<Void> connectFutureTask;

    private List<BinlogEventWrapper> dataList = Collections.synchronizedList(new ArrayList<>());

    public static final String BINLOG_FILE_NAME_KEY = "binlog-file-name";

    public static final String BINLOG_POSITION_KEY = "binlog-position";

    public BinlogDataSource(BinlogDataSourceConfig binlogDataSourceConfig) {
        super(binlogDataSourceConfig);
        this.binlogDataSourceConfig = binlogDataSourceConfig;
        //连接MySQL获取表元信息
        initTableColumnInfo();
        BinaryLogClient client = new BinaryLogClient(binlogDataSourceConfig.getHost(), binlogDataSourceConfig.getPort(), binlogDataSourceConfig.getUserName(), binlogDataSourceConfig.getPassword());
        client.setServerId(binlogDataSourceConfig.getServerId());
        if (binlogDataSourceConfig.getKeepAliveInterval() != null)
            client.setKeepAliveInterval(binlogDataSourceConfig.getKeepAliveInterval());
        this.client = client;
    }

    private void initTableColumnInfo() {
        tableColumnInfo = new ConcurrentHashMap<>();
        tableIdColumnInfoMap = new ConcurrentHashMap<>();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 保存当前注册的表的colum信息
            Connection connection = DriverManager.getConnection("jdbc:mysql://" + binlogDataSourceConfig.getHost() + ":" + binlogDataSourceConfig.getPort(), binlogDataSourceConfig.getUserName(), binlogDataSourceConfig.getPassword());
            // 执行sql
            String preSql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_KEY, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS";
            PreparedStatement ps = connection.prepareStatement(preSql);
            ResultSet rs = ps.executeQuery();
            Map<String, Column> map = new HashMap<>(rs.getRow());
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEMA");
                String tableName = rs.getString("TABLE_NAME");
                String column = rs.getString("COLUMN_NAME");
                String columnKey = rs.getString("COLUMN_KEY");
                long idx = rs.getLong("ORDINAL_POSITION");
                String dataType = rs.getString("DATA_TYPE");
                if (column != null && idx >= 1) {
                    tableColumnInfo.putIfAbsent(schema, new HashMap<>());
                    tableColumnInfo.get(schema).putIfAbsent(tableName, new ArrayList<>());
                    tableColumnInfo.get(schema).get(tableName).add(new Column(schema, tableName, "PRI".equals(columnKey), idx - 1, column, dataType));
                }
            }
            ps.close();
            rs.close();
        }
        catch (SQLException | ClassNotFoundException e) {
            log.error("无法使用配置的用户获取数据库的元数据信息", e);
            throw new BinlogException("无法使用配置的用户获取数据库的元数据信息", e);
        }
    }

    @Override
    public void init(BaseSettingContext settingContext) {
        ITaskStorage taskStorage = TaskStorageThreadLocal.get();
        String binlogFileName = (String) taskStorage.getFromCache(BINLOG_FILE_NAME_KEY);
        binlogFileName = binlogFileName == null ? binlogDataSourceConfig.getBinlogFileName() : binlogFileName;
        Long binlogPosition = (Long) taskStorage.getFromCache(BINLOG_POSITION_KEY);
        binlogPosition = binlogPosition == null ? binlogDataSourceConfig.getBinlogPosition() : binlogPosition;
        if (binlogFileName != null && binlogPosition != null) {
            client.setBinlogFilename(binlogFileName);
            client.setBinlogPosition(binlogPosition);
        }
        this.eventBlockingQueue = new LinkedBlockingQueue<>(binlogDataSourceConfig.getMaxPollRecords());
        client.registerEventListener(event -> {
            try {
                eventBlockingQueue.put(event);
            }
            catch (InterruptedException e) {
                log.error("", e);
            }
        });
        this.connectFutureTask = new FutureTask<>(() -> {
            client.connect();
            return null;
        });
        new Thread(connectFutureTask).start();
        try {
            connectFutureTask.get(3, TimeUnit.SECONDS);
        }
        catch (TimeoutException time) {
            //忽略
        }
        catch (Exception e) {
            throw new BinlogException("连接MySQL binlog发生异常", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.disconnect();
        }
    }

    @Override
    public Collection<BinlogEventWrapper> doPoll(BaseSettingContext baseSettingContext) throws TheEndException {
        while (dataList.size() < binlogDataSourceConfig.getMaxPollRecords()) {
            try {
                Event event = eventBlockingQueue.poll(20, TimeUnit.MILLISECONDS);
                if (event == null)
                    return dataList;
                EventData data = event.getData();
                if (data instanceof RotateEventData) {
                    //binlog文件切换
                    this.binlogFileName = ((RotateEventData) data).getBinlogFilename();
                }
                if (data instanceof TableMapEventData) {
                    //表结构映射
                    TableMapEventData tableMapEventData = (TableMapEventData) data;
                    Long tableId = tableMapEventData.getTableId();
                    String database = tableMapEventData.getDatabase();
                    String table = tableMapEventData.getTable();
                    List<Column> columnList = tableColumnInfo.get(database).get(table);
                    if (tableIdColumnInfoMap.get(tableId) == null) {
                        columnList.sort(Comparator.comparing(Column::getIndex));
                        tableIdColumnInfoMap.put(tableId, columnList);
                    }
                }
                if (data instanceof QueryEventData) {
                    //表结构变更，重新初始化表结构信息
                    initTableColumnInfo();
                }
                dataList.add(new BinlogEventWrapper(this.binlogFileName, event, tableIdColumnInfoMap));
            }
            catch (InterruptedException e) {
                break;
            }
        }
        return dataList;
    }

    @Override
    public String dataSourceType() {
        return "mysql-binlog";
    }

    @Override
    public void commit(BinlogEventWrapper eventWrapper, BaseSettingContext baseSettingContext) {
        dataList.remove(eventWrapper);
        //持久化记录
        TaskStorageThreadLocal.get().setToCache(BINLOG_FILE_NAME_KEY, System.currentTimeMillis(), eventWrapper.getBinlogFileName());
        TaskStorageThreadLocal.get().setToCache(BINLOG_POSITION_KEY, System.currentTimeMillis(), ((EventHeaderV4) eventWrapper.getEvent().getHeader()).getNextPosition());
    }

    @Override
    public void commit(Iterable<BinlogEventWrapper> eventWrappers, BaseSettingContext baseSettingContext) {
        if (eventWrappers instanceof List) {
            List<BinlogEventWrapper> eventWrapperList = (List<BinlogEventWrapper>) eventWrappers;
            BinlogEventWrapper lastBinlogEventWrapper = eventWrapperList.get(eventWrapperList.size() - 1);
            dataList.removeAll(eventWrapperList);
            long nextPosition = ((EventHeaderV4) lastBinlogEventWrapper.getEvent().getHeader()).getNextPosition();
            if (nextPosition != 0) {
                //有些消息没有nextPosition需要过滤
                TaskStorageThreadLocal.get().setToCache(BINLOG_FILE_NAME_KEY, System.currentTimeMillis(), lastBinlogEventWrapper.getBinlogFileName());
                TaskStorageThreadLocal.get().setToCache(BINLOG_POSITION_KEY, System.currentTimeMillis(), nextPosition);
            }
        }
        else {
            eventWrappers.forEach(binlogEventWrapper -> {
                dataList.remove(binlogEventWrapper);
                long nextPosition = ((EventHeaderV4) binlogEventWrapper.getEvent().getHeader()).getNextPosition();
                if (nextPosition != 0) {
                    TaskStorageThreadLocal.get().setToCache(BINLOG_FILE_NAME_KEY, System.currentTimeMillis(), binlogEventWrapper.getBinlogFileName());
                    TaskStorageThreadLocal.get().setToCache(BINLOG_POSITION_KEY, System.currentTimeMillis(), nextPosition);
                }
            });

        }
    }

    @Override
    public void rollback(BinlogEventWrapper eventWrapper, BaseSettingContext baseSettingContext) {
        //默认就是回滚状态，啥都不需要做
    }

    @Override
    public void rollback(Iterable<BinlogEventWrapper> eventWrappers, BaseSettingContext baseSettingContext) {
        //默认就是回滚状态，啥都不需要做
    }

}
