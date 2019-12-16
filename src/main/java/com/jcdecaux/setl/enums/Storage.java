package com.jcdecaux.setl.enums;

/**
 * StorageType
 */
public enum Storage {
    CSV("com.jcdecaux.setl.storage.connector.CSVConnector"),
    EXCEL("com.jcdecaux.setl.storage.connector.ExcelConnector"),
    PARQUET("com.jcdecaux.setl.storage.connector.ParquetConnector"),
    CASSANDRA("com.jcdecaux.setl.storage.connector.CassandraConnector"),
    DYNAMODB("com.jcdecaux.setl.storage.connector.DynamoDBConnector"),
    JSON("com.jcdecaux.setl.storage.connector.JSONConnector"),
    JDBC("com.jcdecaux.setl.storage.connector.JDBCConnector"),
    OTHER(null);

    private String connectorName;

    Storage(String cls) {
        this.connectorName = cls;
    }

    public String connectorName() {
        return connectorName;
    }

}
