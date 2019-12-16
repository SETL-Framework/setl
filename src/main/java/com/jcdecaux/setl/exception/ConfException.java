package com.jcdecaux.setl.exception;

public class ConfException extends BaseException {

    public ConfException(String errorMessage) {
        super(errorMessage);
    }

    public static class Format extends ConfException {
        /**
         * @param errorMessage error message
         */
        public Format(String errorMessage) {
            super(errorMessage);
        }
    }
}
