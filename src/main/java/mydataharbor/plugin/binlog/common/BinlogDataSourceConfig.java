package mydataharbor.plugin.binlog.common;

import lombok.Data;
import mydataharbor.classutil.classresolver.MyDataHarborMarker;
import mydataharbor.datasource.RateLimitConfig;

import java.util.List;
import java.util.Map;

/**
 * @author xulang
 * @date 2023/1/18
 */
@Data
@MyDataHarborMarker(title = "binlog数据源配置")
public class BinlogDataSourceConfig extends RateLimitConfig {
    @MyDataHarborMarker(title = "主库ip")
    private String host;
    @MyDataHarborMarker(title = "主库端口")
    private int port;
    @MyDataHarborMarker(title = "用户名")
    private String userName;
    @MyDataHarborMarker(title = "密码")
    private String password;
    @MyDataHarborMarker(title = "serverId")
    private Long serverId;
    @MyDataHarborMarker(title = "binlog连接重连频率", des = "当与mysql通信连接断开后，重连频率，单位毫秒，默认一分钟", require = false)
    private Long keepAliveInterval;
    @MyDataHarborMarker(title = "binlog日志文件名称", require = false, des = "可选，如果没有指定从最新的开始消费")
    private String binlogFileName;
    @MyDataHarborMarker(title = "binlog日志位置", require = false, des = "可选，如果没有指定从最新的开始消费")
    private Long binlogPosition;
    @MyDataHarborMarker(title = "单次poll最大返回数量", defaultValue = "200")
    private Integer maxPollRecords = 200;
}
