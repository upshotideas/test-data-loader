package com.upshotideas.testhelper;

/**
 * Allows for better flexibility on how to implement a mode, instead of returning sql strings
 */
@FunctionalInterface
public interface CopyOperation {
    void copy(ConnectionSupplier connectionSupplier);
}
