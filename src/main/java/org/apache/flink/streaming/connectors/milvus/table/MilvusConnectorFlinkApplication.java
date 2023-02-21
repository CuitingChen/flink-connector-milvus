package org.apache.flink.streaming.connectors.milvus.table;//package ict.ac.engine.milvus;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.Factory;

import java.util.Iterator;
import java.util.ServiceLoader;

public class MilvusConnectorFlinkApplication {

    public static void main(String[] args) {
        ServiceLoader<Factory> serviceLoader = ServiceLoader.load(Factory.class);
        Iterator<Factory> it = serviceLoader.iterator();
        while (it.hasNext()) {
            try {
                Factory tf = it.next();
                String id = tf.factoryIdentifier();
                System.out.println(id);
            } catch (Exception e) {
                System.out.println(e.toString());
            }

        }
    }

}
