package com.jcdecaux.datacorp.spark.enums;

public enum ValueType {
    STRING("string"),
    DATETIME("timestamp"),
    DATE("date"),
    NUMBER("number"),
    SET("set"),
    COLUMN("column");

    private final String value;

    ValueType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
