package com.upshotideas.testhelper;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionSupplier {
    Connection getConnection();
}
