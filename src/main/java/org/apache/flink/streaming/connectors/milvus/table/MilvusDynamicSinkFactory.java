package org.apache.flink.streaming.connectors.milvus.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * @author chencuiting@ict.ac.cn
 **/
public class MilvusDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "milvus";

    public MilvusDynamicSinkFactory() {

    }


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();
        return new MilvusDynamicSink(context.getCatalogTable().getSchema().toPhysicalRowDataType(), options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(ConfigOptionUtil.HOST);
        required.add(ConfigOptionUtil.PORT);
        return required;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ConfigOptionUtil.COLL_NAME);
        options.add(ConfigOptionUtil.PARTITION_NAME);
        return options;
    }
}
