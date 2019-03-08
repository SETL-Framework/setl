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
}
