package com.upshotideas.testhelper;

/**
 * There are two operating modes available. H2_BUILT_IN is implemented by {@link com.upshotideas.testhelper.filereader.H2BuiltInMode}
 * And the CUSTOM mode is implemented by {@link com.upshotideas.testhelper.filereader.CustomCSVReaderMode}
 */
public enum OperatingMode {
    H2_BUILT_IN,
    CUSTOM
}
