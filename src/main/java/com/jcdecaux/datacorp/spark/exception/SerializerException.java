package com.jcdecaux.datacorp.spark.exception;

public class SerializerException extends RuntimeException {

    public SerializerException(String errorMessage) {
        super(errorMessage);
    }

    public static class Format extends SerializerException {
        /**
         * @param errorMessage
         */
        public Format(String errorMessage) {
            super(errorMessage);
        }
    }
}
