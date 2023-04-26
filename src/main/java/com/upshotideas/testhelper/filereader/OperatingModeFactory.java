package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.OperatingMode;

public class OperatingModeFactory {
    public static IOperatingMode getOperatingModeHandler(OperatingMode mode) {
        switch (mode) {
            case CUSTOM_READ:
                return new CustomCSVReaderMode();
            case H2_BUILT_IN:
            default:
                return new H2BuiltInMode();
        }
    }

    private OperatingModeFactory() {
        // blocking construction
    }
}
