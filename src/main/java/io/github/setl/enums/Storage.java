package io.github.setl.enums;

/**
 * StorageType
 */
public enum Storage {
    CSV("io.github.setl.storage.connector.CSVConnector"),
    EXCEL("io.github.setl.storage.connector.ExcelConnector"),
    PARQUET("io.github.setl.storage.connector.ParquetConnector"),
    DELTA("io.github.setl.storage.connector.DeltaConnector"),
    CASSANDRA("io.github.setl.storage.connector.CassandraConnector"),
    DYNAMODB("io.github.setl.storage.connector.DynamoDBConnector"),
    JSON("io.github.setl.storage.connector.JSONConnector"),
    JDBC("io.github.setl.storage.connector.JDBCConnector"),
    STRUCTURED_STREAMING("io.github.setl.storage.connector.StructuredStreamingConnector"),
    OTHER(null);

    private String connectorName;

    Storage(String cls) {
        this.connectorName = cls;
    }

    public String connectorName() {
        return connectorName;
    }

}
