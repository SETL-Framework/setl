package com.jcdecaux.setl.exception;

public class InvalidConnectorException extends BaseException {
    public InvalidConnectorException() {
    }

    public InvalidConnectorException(String message) {
        super(message);
    }

    public InvalidConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidConnectorException(Throwable cause) {
        super(cause);
    }

    public InvalidConnectorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
