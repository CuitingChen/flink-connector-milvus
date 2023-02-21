package org.apache.flink.streaming.connectors.milvus.table;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * @author chencuiting@ict.ac.cn
 **/
public class ConfigOptionUtil {
    public static final ConfigOption<String> HOST =
            key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "milvus host");
    public static final ConfigOption<Integer> PORT =
            key("port")
                    .intType()
                    .defaultValue(19530)
                    .withDescription(
                            "milvus port");

    public static final ConfigOption<String> COLL_NAME =
            key("collName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "collection name");

    public static final ConfigOption<String> PARTITION_NAME =
            key("partitionName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "partition name");
}
