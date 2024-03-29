package com.upshotideas.testhelper;

import lombok.Builder;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.upshotideas.testhelper.Functions.generateTableSqls;
import static com.upshotideas.testhelper.Functions.getOrderedListOfFiles;

/**
 * Loads all the available csv files located at a given path, ordered in declared order into given
 * DB using the given connection!
 * <p></p>
 * Exposes handy APIs to control how the data is loaded, removed, or reloaded, making it easy to test.
 */
public class TestDataLoader {
    private static final String DEFAULT_DATA_PATH = "src/test/resources/data";
    /**
     * Any jdbc connection to the db in use, preferably h2.
     */
    private final Supplier<Connection> connectionSupplier;

    private String dataPath;
    private OperatingMode operatingMode;

    private Map<String, Consumer<Supplier<Connection>>> tableSqls = Collections.emptyMap();

    /**
     * Constructs the data loader with given connection, of any DB, unlike that of other constructors.
     * dataPath can be any directory, as long at is exists and is readable. It ideally should be in the test/resources/data
     * but any other accessible path can be used.
     * <p></p>
     * If you are using H2 and the data files are in standard location, check other simpler constructors.
     * <p></p>
     *
     * @param connectionSupplier:    Expects an active connection to any DB, as long at is supports the regular insert statements!
     * @param dataPath:      Expects the path to point to a directory.
     * @param operatingMode: Check ${@link OperatingMode} for available modes and their details.
     */
    @Builder
    public TestDataLoader(Supplier<Connection> connectionSupplier, String dataPath, OperatingMode operatingMode) {
        if (connectionSupplier == null) {
            throw new TestDataLoaderException("Connection cannot be null.");
        }
        this.connectionSupplier = connectionSupplier;

        if (dataPath == null) {
            this.dataPath = DEFAULT_DATA_PATH;
        } else {
            this.dataPath = dataPath;
        }

        if (operatingMode == null) {
            this.operatingMode = OperatingMode.H2_BUILT_IN;
        } else {
            this.operatingMode = operatingMode;
        }

        LinkedHashMap<String, Path> orderedFiles = getOrderedListOfFiles(this.dataPath);
        this.tableSqls = generateTableSqls(orderedFiles, this.operatingMode);
    }

    /**
     * This constructor assumes that the data is available at the default location, "src/test/resources/data" and
     * that the connection is to an H2 database.
     *
     * @param connectionSupplier: Expects H2 connection.
     */
    public TestDataLoader(Supplier<Connection> connectionSupplier) {
        this(connectionSupplier, DEFAULT_DATA_PATH, OperatingMode.H2_BUILT_IN);
    }

    /**
     * Loads the csv data into tables. Assumes that the tables are already empty.
     * Uses all CSV files (and therefore tables) available in the data directory.
     */
    public void loadTables() {
        loadData(this.tableSqls.entrySet().stream());
    }

    private void loadData(Stream<Map.Entry<String, Consumer<Supplier<Connection>>>> tables) {
        tables.forEach(entry -> entry.getValue().accept(connectionSupplier));
    }

    /**
     * Same as the ${@link #loadTables()} method but with an option to pass the subset of the tables to be loaded.
     * This method expects the names of the tables and not the files.
     *
     * @param tables: note that this is a list of table names and not filenames.
     */
    public void loadTables(List<String> tables) {
        if (tables.isEmpty()) {
            loadTables();
        } else {
            Stream<Map.Entry<String, Consumer<Supplier<Connection>>>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            loadData(filteredTables);
        }
    }

    /**
     * Clears data from all the tables identified by the files in the data directory
     */
    public void clearTables() {
        ArrayList<Map.Entry<String, Consumer<Supplier<Connection>>>> entries = new ArrayList<>(this.tableSqls.entrySet());
        Collections.reverse(entries);
        Stream<Map.Entry<String, Consumer<Supplier<Connection>>>> tables = entries.stream();
        clearData(tables);
    }

    private void clearData(Stream<Map.Entry<String, Consumer<Supplier<Connection>>>> tables) {
        tables.forEach(entry -> {
            try (Connection connection = this.connectionSupplier.get();
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate("delete from " + entry.getKey() + ";");
                Functions.commitConnection(connection);
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        });
    }

    /**
     * Same as the ${@link #clearTables()}, but additionally allows for clearing a subset of the tables.
     *
     * @param tables: note that this is a list of table names and not filenames.
     */
    public void clearTables(List<String> tables) {
        if (tables.isEmpty()) {
            clearTables();
        } else {
            Stream<Map.Entry<String, Consumer<Supplier<Connection>>>> filteredTables = this.tableSqls.entrySet().stream()
                    .filter(e -> tables.contains(e.getKey()));
            clearData(filteredTables);
        }
    }

    /**
     * Reloads data for all tables identified by the CSV files in the data directory.
     */
    public void reloadTables() {
        this.clearTables();
        this.loadTables();
    }

    /**
     * Same as the ${@link #reloadTables()} but with option to reload data for only a subset of the tables.
     *
     * @param tables: note that this is a list of table names and not filenames.
     */
    public void reloadTables(List<String> tables) {
        if (tables.isEmpty()) {
            reloadTables();
        } else {
            this.clearTables(tables);
            this.loadTables(tables);
        }
    }
}
