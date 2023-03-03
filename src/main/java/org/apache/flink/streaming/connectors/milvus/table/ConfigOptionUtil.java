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

    public static final ConfigOption<String> COLLECTION =
            key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "collection name");

    public static final ConfigOption<String> PARTITION =
            key("partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "partition name");

    public static final ConfigOption<Integer> MAX_INSERT_CACHE_SIZE =
            key("maxInsertCacheSize")
                    .intType()
                    .defaultValue(2000)
                    .withDescription(
                            "max cache size to insert");

    public static final ConfigOption<Long> MAX_INSERT_CACHE_TIME_INTERVAL =
            key("maxInsertCacheTimeInterval")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription(
                            "max cache time interval to insert");
}
