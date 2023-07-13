package com.upshotideas.testhelper;

/**
 * There are two operating modes available.
 */
public enum OperatingMode {
    /**
     * H2_BUILT_IN is implemented by {@link com.upshotideas.testhelper.filereader.H2BuiltInMode}
     */
    H2_BUILT_IN,

    /**
     * CUSTOM mode is implemented by {@link com.upshotideas.testhelper.filereader.CustomCSVReaderMode}
     */
    CUSTOM,

    /**
     * POSTGRESQL_COPY is implemeted by {@link com.upshotideas.testhelper.filereader.PostgreSQLBuiltInMode}
     */
    POSTGRESQL_COPY
}
