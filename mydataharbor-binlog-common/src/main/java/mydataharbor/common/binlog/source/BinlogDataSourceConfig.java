package mydataharbor.common.binlog.source;

import lombok.Data;
import mydataharbor.classutil.classresolver.MyDataHarborMarker;
import mydataharbor.datasource.RateLimitConfig;

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
    @MyDataHarborMarker(title = "binlog日志文件名称", require = false, des = "可选，如果没有指定从最新的开始消费")
    private String binlogFileName;
    @MyDataHarborMarker(title = "binlog日志位置", require = false, des = "可选，如果没有指定从最新的开始消费")
    private Long binlogPosition;
}
