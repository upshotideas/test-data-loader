package com.upshotideas.testhelper.filereader;

import com.upshotideas.testhelper.Functions;
import com.upshotideas.testhelper.TableOperationTuple;
import com.upshotideas.testhelper.TestDataLoaderException;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This mode allows for some dynamic components to the CSV. One can embed SQL snippets in between __sql__ tags and
 * the generated insert statements will be such that they allow for executing these sqls, enabling some data to be
 * dynamic.
 * <p/>
 * Since this is a custom method, there are some constraints this puts on the data's format; the data needs to be
 * properly formatted, always enclosed in double quotes, if it is of type string.
 */
class CustomCSVReaderMode implements IOperatingMode {
    @Override
    public TableOperationTuple generateTableSql(Map.Entry<String, Path> e) throws IOException {
        List<String> fileLines = FileUtils.readLines(e.getValue().toFile(), Charset.defaultCharset());
        if (fileLines.isEmpty()) {
            return Functions.getNoopOperation(e.getKey());
        }

        return new TableOperationTuple(e.getKey(), this.generateCopyOperation(e.getKey(), fileLines));
    }

    private Consumer<Supplier<Connection>> generateCopyOperation(String tableName, List<String> fileLines) {
        String insertStmt = formInsertStatement(tableName, fileLines);

        return connectionSupplier -> {
            try (Connection connection = connectionSupplier.get();
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate(insertStmt);
                Functions.commitConnection(connection);
            } catch (SQLException e) {
                throw new TestDataLoaderException(e);
            }
        };
    }

    private static String formInsertStatement(String tableName, List<String> fileLines) {
        String columns = fileLines.remove(0);

        List<String> inserts = fileLines.stream()
                .map(s -> s.replaceAll("\"?__sql__\"?", "")
                        .replaceAll("(?<!\")\"(?!\")", "'")
                        .replaceAll("\"\"", "\"")
                )
                .map(row -> String.format("insert into %s(%s) values (%s);", tableName, columns, row))
                .collect(Collectors.toList());

        return String.join("", inserts);
    }
}
