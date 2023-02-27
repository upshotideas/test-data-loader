package com.upshotideas.testhelper;

public class TestDataLoaderException extends RuntimeException {
    public TestDataLoaderException() {
        super();
    }

    public TestDataLoaderException(Exception originalException) {
        super(originalException);
    }
}
