package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.OperatingMode;

/**
 * Creates instance of the applicable mode implementation based on provided ${@link OperatingMode}.
 */
public class OperatingModeFactory {
    /**
     * Returns instance of the ${@link IOperatingMode} implementation.
     *
     * @param mode accepts an enum of tye ${@link OperatingMode}
     * @return returns instance of ${@link IOperatingMode}
     */
    public static IOperatingMode getOperatingModeHandler(OperatingMode mode) {
        switch (mode) {
            case CUSTOM:
                return new CustomCSVReaderMode();
            case POSTGRESQL_COPY:
                return new PostgreSQLBuiltInMode();
            case H2_BUILT_IN:
            default:
                return new H2BuiltInMode();
        }
    }

    private OperatingModeFactory() {
        // blocking construction
    }
}
