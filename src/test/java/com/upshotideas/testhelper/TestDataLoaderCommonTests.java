package com.upshotideas.testhelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.upshotideas.testhelper.Functions.prefixNumericComparatorGenerator;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class TestDataLoaderCommonTests extends TestHelper {

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldLoadTheData_WhenDataFileIsFound(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);

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
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);
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
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);

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
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);
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
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);

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
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);
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
        List<Path> sorted = paths.stream().sorted(prefixNumericComparatorGenerator()).collect(Collectors.toList());

        assertAll(() -> sorted.get(0).getFileName().equals("9.testfile.csv"),
                () -> sorted.get(1).getFileName().equals("10.testfile2.csv"),
                () -> sorted.get(2).getFileName().equals("11.testfile3.csv")
        );
    }

    @Test
    void ensureDefaultsStick() throws NoSuchFieldException {
        TestDataLoader dataLoader = TestDataLoader.builder().connectionSupplier(connectionSupplier).build();
        Field dataPath = TestDataLoader.class.getDeclaredField("dataPath");
        dataPath.setAccessible(true);

        Field operatingMode = TestDataLoader.class.getDeclaredField("operatingMode");
        operatingMode.setAccessible(true);

        assertAll(
                () -> assertEquals("src/test/resources/data", dataPath.get(dataLoader)),
                () -> assertEquals(OperatingMode.H2_BUILT_IN, operatingMode.get(dataLoader))
        );
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearAndReloadTables_whenReloadIsInvoked(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = TestDataLoader.builder()
                .connectionSupplier(connectionSupplier).dataPath(dataPath)
                .operatingMode(operatingMode).build();

        String sqlStmt = "insert into client values(98, 'someName2', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');" +
                "insert into client values(99, 'someName3', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');";
        try (Connection connection = this.connectionSupplier.get();
             Statement statement = connection.createStatement();) {
            int i = statement.executeUpdate(sqlStmt);
        }
        assertAll(() -> assertEquals(2, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.reloadTables();
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearAndReloadSelectedTables_whenReloadIsInvokedWithSelectedTables(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = TestDataLoader.builder()
                .connectionSupplier(connectionSupplier).dataPath(dataPath)
                .operatingMode(operatingMode).build();

        String sqlStmt = "insert into client values(98, 'someName2', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');" +
                "insert into client values(99, 'someName3', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');";
        try (Connection connection = this.connectionSupplier.get();
             Statement statement = connection.createStatement();) {
            int i = statement.executeUpdate(sqlStmt);
        }
        assertAll(() -> assertEquals(2, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.reloadTables(Arrays.asList("second_table"));
        assertAll(() -> assertEquals(2, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsProvider")
    void shouldClearAndReloadTables_whenReloadIsInvokedWithEmptyArray(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = TestDataLoader.builder()
                .connectionSupplier(connectionSupplier).dataPath(dataPath)
                .operatingMode(operatingMode).build();

        String sqlStmt = "insert into client values(98, 'someName2', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');" +
                "insert into client values(99, 'someName3', 'CREATED', 'nikhil', '2023-02-27', 'nikhil', '2023-02-27');";
        try (Connection connection = this.connectionSupplier.get();
             Statement statement = connection.createStatement();) {
            int i = statement.executeUpdate(sqlStmt);
        }
        assertAll(() -> assertEquals(2, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.reloadTables(Arrays.asList());
        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }

    @ParameterizedTest
    @MethodSource("paramsNoSequenceProvider")
    void shouldLoadFileInWhateverOrder_WhenFilesMissSequenceNumber(String dataPath, OperatingMode operatingMode) throws SQLException {
        TestDataLoader dataLoader = new TestDataLoader(connectionSupplier, dataPath, operatingMode);

        assertAll(() -> assertEquals(0, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));

        dataLoader.loadTables();

        assertAll(() -> assertEquals(1, runQueryForCount("select count(0) as count from client;")),
                () -> assertEquals(1, runQueryForCount("select count(0) as count from second_table;")),
                () -> assertEquals(0, runQueryForCount("select count(0) as count from third_table;")));
    }
}
