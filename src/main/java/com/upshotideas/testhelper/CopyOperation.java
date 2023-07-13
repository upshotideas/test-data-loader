package com.upshotideas.testhelper;

import java.sql.Connection;

/**
 * Allows for better flexibility on how to implement a mode, instead of returning sql strings
 */
@FunctionalInterface
public interface CopyOperation {
    void copy(Connection connection);
}
