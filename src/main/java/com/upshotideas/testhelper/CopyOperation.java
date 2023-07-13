package com.upshotideas.testhelper;

import java.sql.Connection;

@FunctionalInterface
public interface CopyOperation {
    void copy(Connection connection);
}
