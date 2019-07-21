package com.jcdecaux.datacorp.spark.exception;

public class ConfException extends BaseException {

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
