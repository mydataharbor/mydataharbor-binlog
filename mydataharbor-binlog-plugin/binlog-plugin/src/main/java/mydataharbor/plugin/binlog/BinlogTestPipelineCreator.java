package mydataharbor.plugin.binlog;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import mydataharbor.IDataConverter;
import mydataharbor.IDataPipeline;
import mydataharbor.IDataPipelineCreator;
import mydataharbor.IDataSink;
import mydataharbor.IProtocolData;
import mydataharbor.common.binlog.BinlogDataSourceConfig;
import mydataharbor.converter.data.OriginalDataConverter;
import mydataharbor.converter.protocol.OriginalProtocolDataConverter;
import mydataharbor.exception.ResetException;
import mydataharbor.pipeline.CommonDataPipeline;
import mydataharbor.plugin.base.util.JsonUtil;
import mydataharbor.plugin.binlog.source.BinlogDataSource;
import mydataharbor.plugin.binlog.source.protocol.BinlogCUDProtocolConverter;
import mydataharbor.setting.BaseSettingContext;

import java.io.IOException;
import java.util.List;

import org.pf4j.Extension;
import org.pf4j.ExtensionPoint;

/**
 * @author xulang
 * @date 2023/1/30
 */
@Extension
public class BinlogTestPipelineCreator implements IDataPipelineCreator<BinlogTestPipelineCreator.BinlogTestPipelineCreatorConfig, BaseSettingContext>, ExtensionPoint {

    @Override
    public String type() {
        return "BinlogTestPipelineCreator";
    }

    @Override
    public IDataPipeline createPipeline(BinlogTestPipelineCreatorConfig binlogTestPipelineCreatorConfig, BaseSettingContext baseSettingContext) throws Exception {
        CommonDataPipeline commonDataPipeline = CommonDataPipeline.builder()
                .dataSource(new BinlogDataSource(binlogTestPipelineCreatorConfig.binlogDataSourceConfig))
                .protocolDataConverter(new BinlogCUDProtocolConverter())
                .dataConverter((protocolData, baseSettingContext1) -> protocolData)
                .sink(new ObjectSink())
                .settingContext(baseSettingContext)
                .build();
        return commonDataPipeline;
    }

    @Override
    public <T> T parseJson(String s, Class<T> aClass) {
        return JsonUtil.jsonToObject(s, aClass);
    }

    @Data
    public static class BinlogTestPipelineCreatorConfig{
        private BinlogDataSourceConfig binlogDataSourceConfig;
    }

    @Slf4j
    public static class ObjectSink implements IDataSink<Object, BaseSettingContext> {
        @Override
        public String name() {
            return "测试写入器";
        }

        @Override
        public WriterResult write(Object record, BaseSettingContext settingContext) throws ResetException {
            log.info("测试写入器单条写入:{}", record);
            return WriterResult.builder().commit(true).success(true).msg("ok").build();
        }

        @Override
        public WriterResult write(List<Object> records, BaseSettingContext settingContext) throws ResetException {
            log.info("测试写入器批量写入:{}", records);
            return WriterResult.builder().commit(true).success(true).msg("ok").build();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
