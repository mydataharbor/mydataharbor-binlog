package mydataharbor.source.binlog;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

/**
 * @author xulang
 * @date 2023/1/18
 */
public class Test {

    public static void main(String[] args) throws InterruptedException {
        BinaryLogClient client = new BinaryLogClient("127.0.01", 3306, "root", "123456");
        client.setServerId(2);
        //client.setBinlogFilename("XULANG01-bin.000001");
        //client.setBinlogPosition(1);
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
            } else if (data instanceof WriteRowsEventData) {
                System.out.println("Insert:");
                System.out.println(data.toString());
            } else if (data instanceof DeleteRowsEventData) {
                System.out.println("Delete:");
                System.out.println(data.toString());
            }else {
                System.out.println("other");
                System.out.println(event.toString());
            }
        });

        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
