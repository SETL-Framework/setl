package com.jcdecaux.datacorp.spark.enums;

/**
 * StorageType
 */
public enum Storage {
    CSV("com.jcdecaux.datacorp.spark.storage.connector.CSVConnector"),
    EXCEL("com.jcdecaux.datacorp.spark.storage.connector.ExcelConnector"),
    PARQUET("com.jcdecaux.datacorp.spark.storage.connector.ParquetConnector"),
    CASSANDRA("com.jcdecaux.datacorp.spark.storage.connector.CassandraConnector"),
    DYNAMODB("com.jcdecaux.datacorp.spark.storage.connector.DynamoDBConnector"),
    JSON("com.jcdecaux.datacorp.spark.storage.connector.JSONConnector"),
    JDBC("com.jcdecaux.datacorp.spark.storage.connector.JDBCConnector"),
    OTHER(null);

    private String connectorName;

    Storage(String cls) {
        this.connectorName = cls;
    }

    public String connectorName() {
        return connectorName;
    }

}
