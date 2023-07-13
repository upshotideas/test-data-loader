package com.upshotideas.testhelper;


import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class H2ModeTest extends TestDataLoaderCommonTests {

    @BeforeEach
    public void setupDb() {
        try {
            this.connection = DriverManager.getConnection("jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;", "sa", null);
            File createStmtFile = new File(H2ModeTest.class.getClassLoader().getResource("createTables.sql").toURI());
            String createstmt = FileUtils.readFileToString(createStmtFile, Charset.defaultCharset());

            this.connection.createStatement().executeUpdate(createstmt);
            this.connection.commit();
        } catch (SQLException | IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void teardown() {
        try {
            this.connection.createStatement().execute("SHUTDOWN");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldInsertJSONProperly(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);
        dataLoader.loadTables();

        String actual = runQueryForSelectedStr("select json_col as selected_str from fourth_table;");
        assertAll(() -> assertEquals("\"{ \\\"region\\\":  \\\"us-east-2\\\" }\"",
                actual));
    }

    protected static Stream<Arguments> paramsProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data/csvread", OperatingMode.H2_BUILT_IN),
                Arguments.of("src/test/resources/data/customread", OperatingMode.CUSTOM)
        );
    }

    protected static Stream<Arguments> paramsNoSequenceProvider() {
        return Stream.of(
                Arguments.of("src/test/resources/data-no-sequence/csvread", OperatingMode.H2_BUILT_IN),
                Arguments.of("src/test/resources/data-no-sequence/customread", OperatingMode.CUSTOM)
        );
    }
}
