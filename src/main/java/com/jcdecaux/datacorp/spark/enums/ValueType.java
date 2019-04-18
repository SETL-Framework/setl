package com.jcdecaux.datacorp.spark.enums;

public enum ValueType {
    STRING("string"),
    DATETIME("datetime"),
    DATE("date"),
    NUMBER("number");

    private final String value;

    ValueType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
