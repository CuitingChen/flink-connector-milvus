package org.apache.flink.streaming.connectors.milvus.table;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.codehaus.plexus.util.StringUtils;

import java.util.*;

@Slf4j
public class MilvusSinkFunction extends RichSinkFunction<RowData> {
    private MilvusClient client;

    private final DynamicTableSink.DataStructureConverter converter;
    private final ReadableConfig readableConfig;

    private RowType logicalType;
    private final List<String> fieldNames;

    private long lastInvokeTime;
    private final List<RowData> cache = new ArrayList<>();


    private static int MAX_CACHE_SIZE;

    private static long CACHE_INTERVAL;

    public MilvusSinkFunction(DynamicTableSink.DataStructureConverter converter, ReadableConfig readableConfig, DataType dataType) {

        this.converter = converter;
        this.readableConfig = readableConfig;
        this.logicalType = (RowType) dataType.getLogicalType();
        this.fieldNames = logicalType.getFieldNames();

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(readableConfig.get(ConfigOptionUtil.HOST))
                .withPort(readableConfig.get(ConfigOptionUtil.PORT))
                .build();
        client = new MilvusServiceClient(connectParam);
        lastInvokeTime = System.currentTimeMillis();
        MAX_CACHE_SIZE = readableConfig.get(ConfigOptionUtil.MAX_INSERT_CACHE_SIZE);
        CACHE_INTERVAL = readableConfig.get(ConfigOptionUtil.MAX_INSERT_CACHE_TIME_INTERVAL);

    }


    @Override
    public void invoke(RowData rowData, Context context) {

        cache.add(rowData);
        final long currentTime = System.currentTimeMillis();
        final long interval = currentTime - lastInvokeTime;

        if(cache.size() >= MAX_CACHE_SIZE || interval >= CACHE_INTERVAL) {
            if(CollectionUtils.isEmpty(cache)) {
                return;
            }
            //handle cache data
            final Map<String, InsertParam.Field> insertFieldMap = new HashMap<>();
            for(String fieldName : fieldNames) {
                InsertParam.Field insertField= new InsertParam.Field(fieldName, new ArrayList<>());
                insertFieldMap.put(fieldName, insertField);
            }
            for(RowData cacheData : cache) {
                appendFieldMap(cacheData, insertFieldMap);
            }
            List<InsertParam.Field> insertFieldList = new ArrayList<>(insertFieldMap.values());

            //build insertParam
            String collectionName = readableConfig.get(ConfigOptionUtil.COLLECTION);
            String partitionName = readableConfig.get(ConfigOptionUtil.PARTITION);
            InsertParam.Builder insertParamBuilder = InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFields(insertFieldList);

            if(!StringUtils.isEmpty(partitionName)) {
                insertParamBuilder.withPartitionName(partitionName);
            }
            InsertParam insertParam = insertParamBuilder.build();

            //insert
            client.insert(insertParam);

            //reset
            cache.clear();
            lastInvokeTime = currentTime;
        }


    }


    public void appendFieldMap(RowData rowData, Map<String, InsertParam.Field> insertFieldMap) {
        Row data = (Row) converter.toExternal(rowData);
        for(String fieldName : fieldNames) {
            int index = logicalType.getFieldIndex(fieldName);
            Object fieldData = data.getField(fieldName);
            List fields = insertFieldMap.get(fieldName).getValues();
            LogicalType rowFieldType = logicalType.getFields().get(index).getType();
            LogicalTypeRoot typeRoot  = rowFieldType.getTypeRoot();
            if(LogicalTypeRoot.ARRAY.equals(typeRoot)) {
                LogicalTypeRoot elementType = ((ArrayType)logicalType.getFields().get(index).getType()).getElementType().getTypeRoot();
                switch (elementType) {
                    case FLOAT:
                        addArrayField(fieldData, (List<List<Float>>) fields);
                        break;
                    case BINARY:
                        addArrayField(fieldData, (List<List<Byte>>) fields);
                        break;
                    default:
                        log.error("could not handle this Element type [{}]", elementType);
                        break;
                }
            } else {
                ((List<Object>)fields).add(fieldData);
            }

        }
    }

    public <T> void addArrayField(Object fieldData, List<List<T>> fields) {
       T[] array = (T[])fieldData;
       List<T> vector = Arrays.asList(array);
       fields.add(vector);
    }


    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }
}
