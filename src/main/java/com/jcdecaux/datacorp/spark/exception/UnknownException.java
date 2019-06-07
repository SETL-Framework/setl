package com.jcdecaux.datacorp.spark.exception;

/**
 * UnknownException
 */
public class UnknownException extends RuntimeException {

    /**
     *
     * @param errorMessage
     */
    public UnknownException(String errorMessage) {
        super(errorMessage);
    }

    public static class Storage extends UnknownException {
        /**
         * @param errorMessage
         */
        public Storage(String errorMessage) {
            super(errorMessage);
        }
    }
}

