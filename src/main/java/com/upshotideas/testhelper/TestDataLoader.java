package com.upshotideas.testhelper;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static com.upshotideas.testhelper.Functions.generateTableSqls;
import static com.upshotideas.testhelper.Functions.getOrderedListOfFiles;

/**
 * Loads all the available csv files located at a given path, ordered in declared order into given
 * DB using the given connection!
 * <p/>
 * Exposes handy APIs to control how the data is loaded, removed, or reloaded, making it easy to test.
 */
public class TestDataLoader {
    private static final String DEFAULT_DATA_PATH = "src/test/resources/data";
    /**
     * Any jdbc connection to the db in use, preferably h2.
     */
    private final Connection connection;

    /**
     * Overrid
     */
    private final String dataPath;
    private final OperatingMode operatingMode;

    private Map<String, String> tableSqls = Collections.emptyMap();

    /**
     * Constructs the data loader with given connection, of any DB, unlike that of other constructors.
     * dataPath can be any directory, as long at is exists and is readable. It ideally should be in the test/resources/data
     * but any other accessible path can be used.
     * <p/>
     * If you are using H2 and the data files are in standard location, check other simpler constructors.
     * <p/>
     * @param connection: Expects an active connection to any DB, as long at is supports the regular insert statements!
     * @param dataPath: Expects the path to point to a directory.
     * @param operatingMode: Check ${@link OperatingMode} for available modes and their details.
     */
    public TestDataLoader(Connection connection, String dataPath, OperatingMode operatingMode) {
        this.connection = connection;
        this.dataPath = dataPath;
        this.operatingMode = operatingMode;

        LinkedHashMap<String, Path> orderedFiles = getOrderedListOfFiles(this.dataPath);
        this.tableSqls = generateTableSqls(orderedFiles, this.operatingMode);
    }

    /**
     * This constructor assumes that the provided connection is to an H2 database.
     *
     * @param connection: Expects H2 connection.
     * @param dataPath: Expects the path to point to a directory.
     */
    public TestDataLoader(Connection connection, String dataPath) {
        this(connection, dataPath, OperatingMode.H2_BUILT_IN);
    }

    /**
     * This constructor assumes that the data is available at the default location, "src/test/resources/data" and
     * that the connection is to an H2 database.
     *
     * @param connection: Expects H2 connection.
     */
    public TestDataLoader(Connection connection) {
        this(connection, DEFAULT_DATA_PATH);
    }

    public void loadTables() {
        loadData(this.tableSqls.entrySet().stream());
    }

    private void loadData(Stream<Map.Entry<String, String>> tables) {
        tables.forEach(entry -> {
            try {
                this.connection.createStatement().executeUpdate(entry.getValue());
                this.connection.commit();
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        });
    }

    public void loadTables(List<String> tables) {
        if (tables.isEmpty()) {
            loadTables();
        } else {
            Stream<Map.Entry<String, String>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            loadData(filteredTables);
        }
    }

    public void clearTables() {
        ArrayList<Map.Entry<String, String>> entries = new ArrayList<>(this.tableSqls.entrySet());
        Collections.reverse(entries);
        Stream<Map.Entry<String, String>> tables = entries.stream();
        clearData(tables);
    }

    private void clearData(Stream<Map.Entry<String, String>> tables) {
        tables.forEach(entry -> {
            try {
                this.connection.createStatement().executeUpdate("delete from " + entry.getKey() + ";");
                this.connection.commit();
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        });
    }

    public void clearTables(List<String> tables) {
        if (tables.isEmpty()) {
            clearTables();
        } else {
            Stream<Map.Entry<String, String>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            clearData(filteredTables);
        }
    }

    public void reloadTables() {
        this.clearTables();
        this.loadTables();
    }

    public void reloadTables(List<String> tables) {
        if (tables.isEmpty()) {
            reloadTables();
        } else {
            this.clearTables(tables);
            this.loadTables(tables);
        }
    }
}
