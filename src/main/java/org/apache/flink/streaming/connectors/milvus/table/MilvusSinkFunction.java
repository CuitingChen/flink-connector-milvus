package org.apache.flink.streaming.connectors.milvus.table;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
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
    private DataType dataType;
    private RowType logicalType;
    private HashMap<Integer, String> fields;

    public MilvusSinkFunction(DynamicTableSink.DataStructureConverter converter, ReadableConfig readableConfig, DataType dataType) {
        this.converter = converter;
        this.readableConfig = readableConfig;
        this.dataType = dataType;

    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        logicalType = (RowType) dataType.getLogicalType();
        fields = new HashMap<>();
        List<RowType.RowField> rowFields = logicalType.getFields();
        int size = rowFields.size();
        for(int i = 0; i < size; i++) {
            fields.put(i, rowFields.get(i).getName());
        }

        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(readableConfig.get(ConfigOptionUtil.HOST))
                .withPort(readableConfig.get(ConfigOptionUtil.PORT))
                .build();
        client = new MilvusServiceClient(connectParam);
    }

//    @Override
//    public void invoke(List<Map<String, Object>> docs, Context context) throws Exception {
//        if(CollectionUtils.isEmpty(docs)) {
//            return;
//        }
//        Set<String> fieldSet = docs.get(0).keySet();
//        Map<String, List<Object>> fieldValues = new HashMap<>();
//        for(Map<String, Object> doc : docs) {
//            for(String fieldName : fieldSet) {
//                if(fieldValues.get(fieldName) == null) {
//                    List<Object> values = new ArrayList<>();
//                    fieldValues.put(fieldName, values);
//                }
//
//                fieldValues.get(fieldName).add(doc.get(fieldName));
//            }
//        }
//
//        List<InsertParam.Field> fieldList = new ArrayList<>();
//        for(Map.Entry<String, List<Object>> entry : fieldValues.entrySet()) {
//            InsertParam.Field field = new InsertParam.Field(entry.getKey(), entry.getValue());
//            fieldList.add(field);
//        }
//
//        InsertParam insertParam = InsertParam.newBuilder()
//                .withCollectionName(readableConfig.get(ConfigOptionUtil.COLL_NAME))
//                .withFields(fieldList)
//                .withPartitionName(readableConfig.get(ConfigOptionUtil.PARTITION_NAME))
//                .build();
//
//        client.insert(insertParam);
//    }

    @Override
    public void invoke(RowData rowData, Context context) {
        Row data = (Row) converter.toExternal(rowData);
        List<InsertParam.Field> fieldList = new ArrayList<>();
        for(Map.Entry<Integer, String> entry : fields.entrySet()) {
            int index = entry.getKey();
            String fieldName = entry.getValue();
            Object fieldData = data.getField(index);
            LogicalTypeRoot typeRoot  = logicalType.getFields().get(index).getType().getTypeRoot();
            InsertParam.Field insertField;
            if(LogicalTypeRoot.ARRAY.equals(typeRoot)) {
                Float[] array = (Float[])fieldData;
                List<Float> vector = Arrays.asList(array);
                List<List<Float>> abc = new ArrayList<>();
                abc.add(vector);
                insertField = new InsertParam.Field(fieldName, abc);
            } else {
                insertField = new InsertParam.Field(fieldName, Arrays.asList(fieldData));
            }
            fieldList.add(insertField);
        }
        String collectionName = readableConfig.get(ConfigOptionUtil.COLL_NAME);
        String partitionName = readableConfig.get(ConfigOptionUtil.PARTITION_NAME);
        InsertParam.Builder insertParamBuilder = InsertParam.newBuilder()
                .withCollectionName(collectionName)
                .withFields(fieldList);

        if(!StringUtils.isEmpty(partitionName)) {
            insertParamBuilder.withPartitionName(partitionName);
        }
        InsertParam insertParam = insertParamBuilder.build();

        client.insert(insertParam);
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }
}
