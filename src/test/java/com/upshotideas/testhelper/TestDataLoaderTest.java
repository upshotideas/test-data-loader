package com.upshotideas.testhelper;


import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDataLoaderTest extends TestHelper {

    @BeforeEach
    public void setupDb()
    {
        try {
            this.connection = DriverManager.getConnection("jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;", "sa", null);
            File createStmtFile = new File(TestDataLoaderTest.class.getClassLoader().getResource("createTables.sql").toURI());
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
    void shouldLoadTheData_WhenDataFileIsFound(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);

        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.loadTables();

        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearTables_WhenInvoked(String dataPath, OperatingMode operatingMode) {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);
        dataLoader.loadTables();
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.clearTables();
        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldLoadTheDataForSpecificTables_WhenDataFileIsFoundAndTablesAreDefined(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);

        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.loadTables(Arrays.asList("client"));

        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearForSpecificTables_WhenITablesAreDefined(String dataPath, OperatingMode operatingMode) {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);
        dataLoader.loadTables();
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.clearTables(Arrays.asList("second_table"));
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldLoadTheDataForAllTables_TablesAreAnEmptyList(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);

        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.loadTables(Collections.emptyList());

        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearForSpecificTables_TablesAreAnEmptyList(String dataPath, OperatingMode operatingMode) {
        TestDataLoader dataLoader = new TestDataLoader(connection, dataPath, operatingMode);
        dataLoader.loadTables();
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.clearTables(Collections.emptyList());
        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @Test
    void shouldOrderFilesInNaturalOrder() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        List<Path> paths = Arrays.asList(Paths.get("11.testfile3.csv"), Paths.get("10.testfile2.csv"), Paths.get("9.testfile.csv"));
        Comparator<Path> prefixNumericComparator = getComparator();
        List<Path> sorted = paths.stream().sorted(prefixNumericComparator).collect(Collectors.toList());

        assertAll(() -> sorted.get(0).getFileName().equals("9.testfile.csv"),
                () -> sorted.get(1).getFileName().equals("10.testfile2.csv"),
                () -> sorted.get(2).getFileName().equals("11.testfile3.csv")
        );
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
}