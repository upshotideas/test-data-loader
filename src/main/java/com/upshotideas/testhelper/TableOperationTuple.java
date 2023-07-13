package com.upshotideas.testhelper;

import lombok.AllArgsConstructor;

/**
 * Just a temporary data container for internal use.
 */
@AllArgsConstructor
public class TableOperationTuple {
    public final String tableName;
    public final CopyOperation operation;
}
