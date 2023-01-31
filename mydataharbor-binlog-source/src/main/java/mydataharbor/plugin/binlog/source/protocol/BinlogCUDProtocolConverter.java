package mydataharbor.plugin.binlog.source.protocol;

import mydataharbor.IProtocolDataConverter;
import mydataharbor.common.binlog.BinlogCUDProtocolConverterConfig;
import mydataharbor.common.binlog.BinlogCUDProtocolData;
import mydataharbor.common.binlog.Column;
import mydataharbor.exception.ResetException;
import mydataharbor.setting.BaseSettingContext;
import mydataharbor.plugin.binlog.source.BinlogEventWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

/**
 * 这个转换器只转换 增删改的事件
 *
 * @author xulang
 * @date 2023/1/30
 */
public class BinlogCUDProtocolConverter implements IProtocolDataConverter<BinlogEventWrapper, BinlogCUDProtocolData, BaseSettingContext> {

    private BinlogCUDProtocolConverterConfig binlogCUDProtocolConverterConfig;

    public BinlogCUDProtocolConverter(BinlogCUDProtocolConverterConfig binlogCUDProtocolConverterConfig){
        this.binlogCUDProtocolConverterConfig = binlogCUDProtocolConverterConfig;
    }

    @Override
    public BinlogCUDProtocolData convert(BinlogEventWrapper binlogEventWrapper, BaseSettingContext baseSettingContext) throws ResetException {
        BinlogCUDProtocolData binlogCUDProtocolData = new BinlogCUDProtocolData();
        binlogCUDProtocolData.setBinlogFileName(binlogEventWrapper.getBinlogFileName());
        binlogCUDProtocolData.setPosition(((EventHeaderV4) binlogEventWrapper.getEvent().getHeader()).getPosition());
        binlogCUDProtocolData.setEventLength(((EventHeaderV4) binlogEventWrapper.getEvent().getHeader()).getEventLength());
        binlogCUDProtocolData.setDataLength(binlogEventWrapper.getEvent().getHeader().getDataLength());
        EventData eventData = binlogEventWrapper.getEvent().getData();
        if (eventData instanceof UpdateRowsEventData) {
            if (!processUpdate(binlogEventWrapper, binlogCUDProtocolData, (UpdateRowsEventData) eventData))
                return null;
        } else if (eventData instanceof WriteRowsEventData) {
            if (!processInsert(binlogEventWrapper, binlogCUDProtocolData, (WriteRowsEventData) eventData))
                return null;
        } else if (eventData instanceof DeleteRowsEventData) {
            if (!processDelete(binlogEventWrapper, binlogCUDProtocolData, (DeleteRowsEventData) eventData))
                return null;
        } else {
            return null;
        }
        return binlogCUDProtocolData;
    }

    public boolean processUpdate(BinlogEventWrapper binlogEventWrapper, BinlogCUDProtocolData binlogCUDProtocolData, UpdateRowsEventData eventData) {
        UpdateRowsEventData updateRowsEventData = eventData;
        binlogCUDProtocolData.setCud(BinlogCUDProtocolData.CUD.U);
        List<Column> columns = binlogEventWrapper.getTableColumnInfo().get(updateRowsEventData.getTableId());
        if (binlogCUDProtocolConverterConfig.getNeedConvertTables() != null) {
            List<String> needConvertTables = binlogCUDProtocolConverterConfig.getNeedConvertTables().get(columns.get(0).getDatabase());
            if (needConvertTables == null)
                return false;
            else if (!needConvertTables.contains(columns.get(0).getTable()))
                return false;
        }
        binlogCUDProtocolData.setColumns(columns);
        binlogCUDProtocolData.setDatabase(columns.get(0).getDatabase());
        binlogCUDProtocolData.setTable(columns.get(0).getTable());
        List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
        List<Map<String, Object>> before = new ArrayList<>();
        List<Map<String, Object>> after = new ArrayList<>();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            Map<String, Object> beforeItem = new HashMap<>();
            for (int i = 0; i < row.getKey().length; i++) {
                beforeItem.put(columns.get(i).getColName(), row.getKey()[i]);
            }
            before.add(beforeItem);
            Map<String, Object> afterItem = new HashMap<>();
            for (int i = 0; i < row.getValue().length; i++) {
                afterItem.put(columns.get(i).getColName(), row.getValue()[i]);
            }
            after.add(afterItem);
        }
        binlogCUDProtocolData.setBefore(before);
        binlogCUDProtocolData.setAfter(after);
        return true;
    }

    public boolean processInsert(BinlogEventWrapper binlogEventWrapper, BinlogCUDProtocolData binlogCUDProtocolData, WriteRowsEventData eventData) {
        WriteRowsEventData writeRowsEventData = eventData;
        binlogCUDProtocolData.setCud(BinlogCUDProtocolData.CUD.C);
        List<Column> columns = binlogEventWrapper.getTableColumnInfo().get(writeRowsEventData.getTableId());
        if (binlogCUDProtocolConverterConfig.getNeedConvertTables() != null) {
            List<String> needConvertTables = binlogCUDProtocolConverterConfig.getNeedConvertTables().get(columns.get(0).getDatabase());
            if (needConvertTables == null)
                return false;
            else if (!needConvertTables.contains(columns.get(0).getTable()))
                return false;
        }
        binlogCUDProtocolData.setColumns(columns);
        binlogCUDProtocolData.setDatabase(columns.get(0).getDatabase());
        binlogCUDProtocolData.setTable(columns.get(0).getTable());
        List<Serializable[]> rows = writeRowsEventData.getRows();
        List<Map<String, Object>> after = new ArrayList<>();
        for (Serializable[] row : rows) {
            Map<String, Object> afterItem = new HashMap<>();
            for (int i = 0; i < row.length; i++) {
                afterItem.put(columns.get(i).getColName(), row[i]);
            }
            after.add(afterItem);
        }
        binlogCUDProtocolData.setAfter(after);
        return true;
    }

    public boolean processDelete(BinlogEventWrapper binlogEventWrapper, BinlogCUDProtocolData binlogCUDProtocolData, DeleteRowsEventData eventData) {
        DeleteRowsEventData deleteRowsEventData = eventData;
        binlogCUDProtocolData.setCud(BinlogCUDProtocolData.CUD.D);
        List<Column> columns = binlogEventWrapper.getTableColumnInfo().get(deleteRowsEventData.getTableId());
        if (binlogCUDProtocolConverterConfig.getNeedConvertTables() != null) {
            List<String> needConvertTables = binlogCUDProtocolConverterConfig.getNeedConvertTables().get(columns.get(0).getDatabase());
            if (needConvertTables == null)
                return false;
            else if (!needConvertTables.contains(columns.get(0).getTable()))
                return false;
        }
        binlogCUDProtocolData.setColumns(columns);
        binlogCUDProtocolData.setDatabase(columns.get(0).getDatabase());
        binlogCUDProtocolData.setTable(columns.get(0).getTable());
        List<Serializable[]> rows = deleteRowsEventData.getRows();
        List<Map<String, Object>> before = new ArrayList<>();
        for (Serializable[] row : rows) {
            Map<String, Object> beforeItem = new HashMap<>();
            for (int i = 0; i < row.length; i++) {
                beforeItem.put(columns.get(i).getColName(), row[i]);
            }
            before.add(beforeItem);
        }
        binlogCUDProtocolData.setBefore(before);
        return true;
    }


}
