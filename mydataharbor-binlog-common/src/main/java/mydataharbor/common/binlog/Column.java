package mydataharbor.common.binlog;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Column {
    private String database; // 数据库
    private String table; // 表
    private Boolean primaryKey = false;//主键
    private Long index;//位置
    private String colName; // 列名
    private String dataType; // 类型
}