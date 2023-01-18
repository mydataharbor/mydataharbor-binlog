package mydataharbor.source.binlog;

import mydataharbor.common.binlog.source.BinlogDataSourceConfig;
import mydataharbor.datasource.AbstractRateLimitDataSource;
import mydataharbor.datasource.RateLimitConfig;
import mydataharbor.exception.TheEndException;
import mydataharbor.setting.BaseSettingContext;

import java.io.IOException;
import java.util.Collection;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

/**
 * @author xulang
 * @date 2023/1/18
 */
public class BinlogDataSource extends AbstractRateLimitDataSource<Event, BaseSettingContext> {

    public BinlogDataSource(BinlogDataSourceConfig binlogDataSourceConfig) {
        super(binlogDataSourceConfig);
        BinaryLogClient client = new BinaryLogClient(binlogDataSourceConfig.getHost(), binlogDataSourceConfig.getPort(), binlogDataSourceConfig.getUserName(), binlogDataSourceConfig.getPassword());
        client.setServerId(binlogDataSourceConfig.getServerId());
        if (binlogDataSourceConfig.getBinlogFileName() != null)
            client.setBinlogFilename(binlogDataSourceConfig.getBinlogFileName());
        if (binlogDataSourceConfig.getBinlogPosition() != null)
            client.setBinlogPosition(binlogDataSourceConfig.getBinlogPosition());
        client.registerEventListener(event -> {
            EventData data = event.getData();
            if (data instanceof TableMapEventData) {
                System.out.println("Table:");
                TableMapEventData tableMapEventData = (TableMapEventData) data;
                System.out.println(tableMapEventData.getTableId());
            }
            if (data instanceof UpdateRowsEventData) {
                System.out.println("Update:");
                System.out.println(data.toString());
            }
            else if (data instanceof WriteRowsEventData) {
                System.out.println("Insert:");
                System.out.println(data.toString());
            }
            else if (data instanceof DeleteRowsEventData) {
                System.out.println("Delete:");
                System.out.println(data.toString());
            }
        });

        try {
            client.connect();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Collection<Event> doPoll(BaseSettingContext baseSettingContext) throws TheEndException {
        return null;
    }

    @Override
    public String dataSourceType() {
        return null;
    }

    @Override
    public void commit(Event event, BaseSettingContext baseSettingContext) {

    }

    @Override
    public void commit(Iterable<Event> iterable, BaseSettingContext baseSettingContext) {

    }

    @Override
    public void rollback(Event event, BaseSettingContext baseSettingContext) {

    }

    @Override
    public void rollback(Iterable<Event> iterable, BaseSettingContext baseSettingContext) {

    }

}
