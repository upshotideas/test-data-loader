package com.upshotideas.testhelper;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Just a temporary data container for internal use.
 */
@AllArgsConstructor
public class TableOperationTuple {
    public final String tableName;
    public final Consumer<Supplier<Connection>> operation;
}
