package mydataharbor.common.binlog;

import lombok.Data;
import mydataharbor.IProtocolData;

import java.util.List;
import java.util.Map;

/**
 * @author xulang
 * @date 2023/1/30
 */
@Data
public class BinlogCUDProtocolData implements IProtocolData {

    /**
     * binlog文件名称
     */
    private String binlogFileName;
    /**
     * 消息位置
     */
    private Long position;
    /**
     * binlog事件大小
     */
    private Long eventLength;
    /**
     * binlog事件数据大小
     */
    private Long dataLength;
    /**
     * 消息类型
     */
    private CUD cud;
    /**
     * 库
     */
    private String database;
    /**
     * 表
     */
    private String table;
    /**
     * 更新和删除类型下会有值
     */
    private List<Map<String, Object>> before;
    /**
     * 更新和创建类型下会有值
     */
    private List<Map<String, Object>> after;
    /**
     * 列定义
     */
    private List<Column> columns;

    @Override
    public String protocolName() {
        return "binlog 增删改协议数据";
    }

    public enum CUD {
        C,//数据创建
        U,//数据更新
        D//数据删除
    }
}
