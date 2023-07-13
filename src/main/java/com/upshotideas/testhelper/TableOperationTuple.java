package com.upshotideas.testhelper;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TableOperationTuple {
    public final String tableName;
    public final CopyOperation operation;
}
