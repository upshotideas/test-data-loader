package com.upshotideas.testhelper;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static com.upshotideas.testhelper.Functions.generateTableSqls;
import static com.upshotideas.testhelper.Functions.getOrderedListOfFiles;

public class TestDataLoader {
    private static final String DEFAULT_DATA_PATH = "src/test/resources/data";
    private final Connection connection;
    private final String dataPath;
    private final OperatingMode operatingMode;

    private Map<String, String> tableSqls = Collections.emptyMap();

    public TestDataLoader(Connection connection, String dataPath, OperatingMode operatingMode) {
        this.connection = connection;
        this.dataPath = dataPath;
        this.operatingMode = operatingMode;

        LinkedHashMap<String, Path> orderedFiles = getOrderedListOfFiles(this.dataPath);
        this.tableSqls = generateTableSqls(orderedFiles, this.operatingMode);
    }

    public TestDataLoader(Connection connection, String dataPath) {
        this(connection, dataPath, OperatingMode.H2_BUILT_IN);
    }
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
