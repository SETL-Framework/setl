package com.jcdecaux.datacorp.spark.exception;

public class ConfException extends RuntimeException {

    public ConfException(String errorMessage) {
        super(errorMessage);
    }

    public static class Format extends ConfException {
        /**
         * @param errorMessage
         */
        public Format(String errorMessage) {
            super(errorMessage);
        }
    }
}
