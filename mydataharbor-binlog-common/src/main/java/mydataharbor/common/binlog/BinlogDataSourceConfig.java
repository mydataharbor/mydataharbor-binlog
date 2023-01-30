package mydataharbor.common.binlog;

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
    @MyDataHarborMarker(title = "监视的对象",des = "可选,如果不填表示监听所有库表，key是库名，value是表集合",require = false)
    private Map<String, List<String>> monitorObject;
    @MyDataHarborMarker(title = "binlog日志文件名称", require = false, des = "可选，如果没有指定从最新的开始消费")
    private String binlogFileName;
    @MyDataHarborMarker(title = "binlog日志位置", require = false, des = "可选，如果没有指定从最新的开始消费")
    private Long binlogPosition;
    @MyDataHarborMarker(title = "单次poll最大返回数量", defaultValue = "200")
    private Integer maxPollRecords = 200;
}
