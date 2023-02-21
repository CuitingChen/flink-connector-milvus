package org.apache.flink.streaming.connectors.milvus.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

/**
 * @author chencuiting@ict.ac.cn
 **/
public class MilvusDynamicSink implements DynamicTableSink {
    private final DataType dataType;
    private final ReadableConfig readableConfig;

    public MilvusDynamicSink(DataType dataType, ReadableConfig readableConfig) {
        this.dataType = dataType;
        this.readableConfig = readableConfig;
    }
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
       return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(dataType);
        return SinkFunctionProvider.of(new MilvusSinkFunction(converter, readableConfig, dataType));
    }

    @Override
    public DynamicTableSink copy() {
        return new MilvusDynamicSink(dataType, readableConfig);
    }

    @Override
    public String asSummaryString() {
        return "mivlus table sink";
    }
}
