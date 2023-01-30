package mydataharbor.plugin.binlog.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mydataharbor.common.binlog.Column;

import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.Event;

/**
 * @author xulang
 * @date 2023/1/30
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BinlogEventWrapper {
    /**
     * binlog文件名称
     */
    private String binlogFileName;
    /**
     * 事件
     */
    private Event event;
    /**
     * 数据库库表信息
     * tableId -> 字段定义
     */
    private Map<Long, List<Column>> tableColumnInfo;
}
