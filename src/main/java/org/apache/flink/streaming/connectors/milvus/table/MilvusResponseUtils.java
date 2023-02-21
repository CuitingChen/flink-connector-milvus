package org.apache.flink.streaming.connectors.milvus.table;

import io.milvus.param.R;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusResponseUtils {

    public static void checkResponse(R response) throws MilvusException {
        if(response == null) {
            throw new MilvusException("response is null");
        }
        if(response.getStatus() != R.Status.Success.getCode()) {
            log.error("milvus response occurs Exception", R.Status.valueOf(response.getStatus()).toString());
            throw new MilvusException(response);
        }
    }
}
