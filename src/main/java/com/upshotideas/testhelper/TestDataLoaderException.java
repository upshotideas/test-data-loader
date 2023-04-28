package com.upshotideas.testhelper;

/**
 * Mainly to convert the checked exceptions thrown by the low level APIs of libraries used, into unchecked exception.
 */
public class TestDataLoaderException extends RuntimeException {
    /**
     * Base constructor
     */
    public TestDataLoaderException() {
        super();
    }

    /**
     * Allows for wrapping and rethrowing existing exceptions.
     * @param originalException whatever was the original cause.
     */
    public TestDataLoaderException(Exception originalException) {
        super(originalException);
    }
}
