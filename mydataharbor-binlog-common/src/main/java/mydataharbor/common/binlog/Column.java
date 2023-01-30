package mydataharbor.common.binlog;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Column {
    public String database; // 数据库
    public String table; // 表
    public Boolean primaryKey = false;//主键
    public Long index;//位置
    public String colName; // 列名
    public String dataType; // 类型
}