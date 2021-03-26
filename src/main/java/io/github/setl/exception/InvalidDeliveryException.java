package io.github.setl.exception;

public class InvalidDeliveryException extends BaseException {

    public InvalidDeliveryException() {
    }

    public InvalidDeliveryException(String message) {
        super(message);
    }

    public InvalidDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidDeliveryException(Throwable cause) {
        super(cause);
    }

    public InvalidDeliveryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
