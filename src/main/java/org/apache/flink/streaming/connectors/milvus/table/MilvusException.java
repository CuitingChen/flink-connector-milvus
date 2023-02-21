package org.apache.flink.streaming.connectors.milvus.table;

import io.milvus.param.R;

public class MilvusException extends RuntimeException {
    private String msg;

    public MilvusException(String msg) {
        super(msg);
    }
    public MilvusException(R.Status responseStatus) {
        super(responseStatus.name());
    }

    public MilvusException(R response) {
        super(response.getMessage() + ";" + response.getException().toString());
    }
}
