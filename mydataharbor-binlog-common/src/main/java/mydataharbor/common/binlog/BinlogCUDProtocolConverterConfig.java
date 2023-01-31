package mydataharbor.common.binlog;

import lombok.Data;
import mydataharbor.classutil.classresolver.MyDataHarborMarker;
import mydataharbor.config.AbstractConfig;

import java.util.List;
import java.util.Map;

/**
 * @author xulang
 * @date 2023/1/31
 */
@Data
public class BinlogCUDProtocolConverterConfig extends AbstractConfig {
    @MyDataHarborMarker(title = "需要转换的表集合", des = "可选,如果不填表示监听所有库表，key是库名，value是表集合，一单设置了那就只转换配置的库表", require = false)
    private Map<String, List<String>> needConvertTables;
}
