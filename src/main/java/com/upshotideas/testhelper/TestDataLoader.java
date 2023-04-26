package com.upshotideas.testhelper;

import com.upshotideas.testhelper.filereader.IOperatingMode;
import com.upshotideas.testhelper.filereader.OperatingModeFactory;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestDataLoader {
    private static final String DEFAULT_DATA_PATH = "src/test/resources/data";
    private static final Pattern FILE_ORDER = Pattern.compile("^(\\d+)\\.");
    private final Connection connection;
    private final String dataPath;
    private final OperatingMode operatingMode;

    private Map<String, String> tableSqls = Collections.emptyMap();

    public TestDataLoader(Connection connection, String dataPath, OperatingMode operatingMode) {
        this.connection = connection;
        this.dataPath = dataPath;
        this.operatingMode = operatingMode;

        LinkedHashMap<String, Path> orderedFiles = getOrderedListOfFiles();
        this.tableSqls = generateTableSqls(orderedFiles);
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

    private Map<String, String> generateTableSqls(LinkedHashMap<String, Path> orderedFiles) {
        IOperatingMode operatingModeHandler = OperatingModeFactory.getOperatingModeHandler(this.operatingMode);
        return orderedFiles.entrySet().stream().map((Map.Entry<String, Path> e) -> {
            try {
                return operatingModeHandler.generateTableSql(e);
            } catch (IOException ex) {
                throw new TestDataLoaderException(ex);
            }
        }).collect(Collectors.toMap(
                kv -> kv.get(0),
                kv -> kv.get(1),
                (k1, k2) -> k1,
                LinkedHashMap::new
        ));
    }

    private LinkedHashMap<String, Path> getOrderedListOfFiles() {
        try (Stream<Path> filesStream = Files.walk(Paths.get(this.dataPath))) {
            return filesStream
                    .filter(p -> Files.isRegularFile(p) && FilenameUtils.isExtension(String.valueOf(p.getFileName()), "csv"))
                    .sorted(prefixNumericComparatorGenerator())
                    .collect(Collectors.toMap(path -> {
                                String fileName = path.getFileName().toString();
                                return fileName.replaceAll(String.valueOf(FILE_ORDER), "")
                                        .replaceAll(".csv", "");
                            }, path -> path,
                            (k1, k2) -> k1,
                            LinkedHashMap::new
                    ));
        } catch (IOException e) {
            throw new TestDataLoaderException(e);
        }
    }

    private static Comparator<Path> prefixNumericComparatorGenerator() {
        return Comparator.comparing(path -> {
            String fileName = path.getFileName().toString();
            Matcher matchedName = FILE_ORDER.matcher(fileName);
            if (matchedName.find()) {
                return Integer.valueOf(matchedName.group(1));
            } else {
                return Integer.MAX_VALUE;
            }
        });
    }
}
